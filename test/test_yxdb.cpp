#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <cstdio>
#include <string>

#include "FieldType.h"
#include "Open_AlteryxYXDB.h"
#include "SrcLib_Replacement.h"

// Helper: create a temporary file path
static std::string temp_path(const std::string& name)
{
	return name + ".yxdb";
}

// Helper: remove a file
static void remove_file(const std::string& path)
{
	std::remove(path.c_str());
}

// Helper: convert U16unit string to std::string for assertions
static std::string to_std_string(const U16unit* p)
{
	return SRC::ConvertToAString(p);
}

TEST_CASE("Write and read back a simple YXDB file", "[roundtrip]")
{
	auto path = temp_path("test_roundtrip");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("IntField"), SRC::E_FT_Int32));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("StrField"), SRC::E_FT_V_String, 100));

	// Write
	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_roundtrip.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		for (int i = 0; i < 10; ++i)
		{
			rec->Reset();
			info[0]->SetFromInt32(rec.Get(), i * 100);
			auto s = SRC::ConvertToWString(std::to_string(i).c_str());
			info[1]->SetFromString(rec.Get(), s.c_str(), static_cast<int>(s.Length()));
			writer.AppendRecord(rec->GetRecord());
		}
		writer.Close();
	}

	// Read
	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_roundtrip.yxdb"));

		REQUIRE(reader.GetNumRecords() == 10);
		REQUIRE(reader.m_recordInfo.NumFields() == 2);

		auto fname0 = to_std_string(reader.m_recordInfo[0]->GetFieldName().c_str());
		auto fname1 = to_std_string(reader.m_recordInfo[1]->GetFieldName().c_str());
		CHECK(fname0 == "IntField");
		CHECK(fname1 == "StrField");

		int count = 0;
		while (const SRC::RecordData* rec = reader.ReadRecord())
		{
			auto intVal = reader.m_recordInfo[0]->GetAsInt32(rec);
			CHECK_FALSE(intVal.bIsNull);
			CHECK(intVal.value == count * 100);

			auto strVal = reader.m_recordInfo[1]->GetAsAString(rec);
			CHECK_FALSE(strVal.bIsNull);
			CHECK(std::string(strVal.value.pValue) == std::to_string(count));

			count++;
		}
		CHECK(count == 10);
	}

	remove_file(path);
}

TEST_CASE("All numeric field types roundtrip correctly", "[fields][numeric]")
{
	auto path = temp_path("test_numeric");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Bool"), SRC::E_FT_Bool));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Byte"), SRC::E_FT_Byte));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Int16"), SRC::E_FT_Int16));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Int32"), SRC::E_FT_Int32));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Int64"), SRC::E_FT_Int64));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Float"), SRC::E_FT_Float));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Double"), SRC::E_FT_Double));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_numeric.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		rec->Reset();
		info[0]->SetFromBool(rec.Get(), true);
		info[1]->SetFromInt32(rec.Get(), 42);
		info[2]->SetFromInt32(rec.Get(), -1234);
		info[3]->SetFromInt32(rec.Get(), 2147483647);
		info[4]->SetFromInt64(rec.Get(), 9223372036854775807LL);
		info[5]->SetFromDouble(rec.Get(), 3.14f);
		info[6]->SetFromDouble(rec.Get(), 2.718281828459045);
		writer.AppendRecord(rec->GetRecord());
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_numeric.yxdb"));

		REQUIRE(reader.GetNumRecords() == 1);

		const SRC::RecordData* rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);

		CHECK(reader.m_recordInfo[0]->GetAsBool(rec).value == true);
		CHECK(reader.m_recordInfo[1]->GetAsInt32(rec).value == 42);
		CHECK(reader.m_recordInfo[2]->GetAsInt32(rec).value == -1234);
		CHECK(reader.m_recordInfo[3]->GetAsInt32(rec).value == 2147483647);
		CHECK(reader.m_recordInfo[4]->GetAsInt64(rec).value == 9223372036854775807LL);
		CHECK_THAT(reader.m_recordInfo[6]->GetAsDouble(rec).value,
				   Catch::Matchers::WithinRel(2.718281828459045, 1e-12));

		CHECK(reader.ReadRecord() == nullptr);
	}

	remove_file(path);
}

TEST_CASE("String field types roundtrip correctly", "[fields][string]")
{
	auto path = temp_path("test_strings");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Fixed"), SRC::E_FT_String, 50));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("VarStr"), SRC::E_FT_V_String, 1000));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("WFixed"), SRC::E_FT_WString, 50));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("VarWStr"), SRC::E_FT_V_WString, 1000));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_strings.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		rec->Reset();
		info[0]->SetFromString(rec.Get(), U16("Hello"));
		info[1]->SetFromString(rec.Get(), U16("Variable length string"));
		info[2]->SetFromString(rec.Get(), U16("Wide fixed"));
		info[3]->SetFromString(rec.Get(), U16("Wide variable string"));
		writer.AppendRecord(rec->GetRecord());
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_strings.yxdb"));

		REQUIRE(reader.GetNumRecords() == 1);

		const SRC::RecordData* rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);

		auto f0 = reader.m_recordInfo[0]->GetAsAString(rec);
		auto f1 = reader.m_recordInfo[1]->GetAsAString(rec);
		auto f2 = reader.m_recordInfo[2]->GetAsAString(rec);
		auto f3 = reader.m_recordInfo[3]->GetAsAString(rec);

		CHECK(std::string(f0.value.pValue) == "Hello");
		CHECK(std::string(f1.value.pValue) == "Variable length string");
		CHECK(std::string(f2.value.pValue) == "Wide fixed");
		CHECK(std::string(f3.value.pValue) == "Wide variable string");
	}

	remove_file(path);
}

TEST_CASE("Null values roundtrip correctly", "[fields][null]")
{
	auto path = temp_path("test_null");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Int"), SRC::E_FT_Int32));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Str"), SRC::E_FT_V_String, 100));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Dbl"), SRC::E_FT_Double));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_null.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();

		// Row 0: all values set
		rec->Reset();
		info[0]->SetFromInt32(rec.Get(), 42);
		info[1]->SetFromString(rec.Get(), U16("hello"));
		info[2]->SetFromDouble(rec.Get(), 3.14);
		writer.AppendRecord(rec->GetRecord());

		// Row 1: all null
		rec->Reset();
		info[0]->SetNull(rec.Get());
		info[1]->SetNull(rec.Get());
		info[2]->SetNull(rec.Get());
		writer.AppendRecord(rec->GetRecord());

		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_null.yxdb"));

		REQUIRE(reader.GetNumRecords() == 2);

		// Row 0: values present
		const SRC::RecordData* rec0 = reader.ReadRecord();
		REQUIRE(rec0 != nullptr);
		CHECK_FALSE(reader.m_recordInfo[0]->GetNull(rec0));
		CHECK_FALSE(reader.m_recordInfo[1]->GetNull(rec0));
		CHECK_FALSE(reader.m_recordInfo[2]->GetNull(rec0));

		// Row 1: all null
		const SRC::RecordData* rec1 = reader.ReadRecord();
		REQUIRE(rec1 != nullptr);
		CHECK(reader.m_recordInfo[0]->GetNull(rec1));
		CHECK(reader.m_recordInfo[1]->GetNull(rec1));
		CHECK(reader.m_recordInfo[2]->GetNull(rec1));
	}

	remove_file(path);
}

TEST_CASE("Date and time field types roundtrip correctly", "[fields][datetime]")
{
	auto path = temp_path("test_datetime");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Date"), SRC::E_FT_Date));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Time"), SRC::E_FT_Time));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("DateTime"), SRC::E_FT_DateTime));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_datetime.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		rec->Reset();
		info[0]->SetFromString(rec.Get(), U16("2024-01-15"));
		info[1]->SetFromString(rec.Get(), U16("13:45:30"));
		info[2]->SetFromString(rec.Get(), U16("2024-01-15 13:45:30"));
		writer.AppendRecord(rec->GetRecord());
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_datetime.yxdb"));

		REQUIRE(reader.GetNumRecords() == 1);

		const SRC::RecordData* rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);

		auto d = reader.m_recordInfo[0]->GetAsAString(rec);
		auto t = reader.m_recordInfo[1]->GetAsAString(rec);
		auto dt = reader.m_recordInfo[2]->GetAsAString(rec);

		CHECK(std::string(d.value.pValue) == "2024-01-15");
		CHECK(std::string(t.value.pValue) == "13:45:30");
		CHECK(std::string(dt.value.pValue) == "2024-01-15 13:45:30");
	}

	remove_file(path);
}

TEST_CASE("Large file with multiple blocks", "[blocks]")
{
	auto path = temp_path("test_large");

	// RecordsPerBlock = 0x10000 = 65536, write more than one block
	const int num_records = 70000;

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("ID"), SRC::E_FT_Int32));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_large.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		for (int i = 0; i < num_records; ++i)
		{
			rec->Reset();
			info[0]->SetFromInt32(rec.Get(), i);
			writer.AppendRecord(rec->GetRecord());
		}
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_large.yxdb"));

		REQUIRE(reader.GetNumRecords() == num_records);

		// Just verify we can read all records without error
		int count = 0;
		while (reader.ReadRecord() != nullptr)
		{
			count++;
		}
		CHECK(count == num_records);
	}

	remove_file(path);
}

TEST_CASE("Random access with GoRecord", "[random-access]")
{
	auto path = temp_path("test_random");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Val"), SRC::E_FT_Int32));

	const int num_records = 200;

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_random.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		for (int i = 0; i < num_records; ++i)
		{
			rec->Reset();
			info[0]->SetFromInt32(rec.Get(), i * 10);
			writer.AppendRecord(rec->GetRecord());
		}
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_random.yxdb"));

		// Seek to record 50
		reader.GoRecord(50);
		const SRC::RecordData* rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);
		CHECK(reader.m_recordInfo[0]->GetAsInt32(rec).value == 500);

		// Seek back to beginning
		reader.GoRecord(0);
		rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);
		CHECK(reader.m_recordInfo[0]->GetAsInt32(rec).value == 0);
	}

	remove_file(path);
}

TEST_CASE("Empty file with schema only", "[edge-case]")
{
	auto path = temp_path("test_empty");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Col"), SRC::E_FT_Int32));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_empty.yxdb"), info.GetRecordXmlMetaData());
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_empty.yxdb"));

		REQUIRE(reader.GetNumRecords() == 0);
		REQUIRE(reader.m_recordInfo.NumFields() == 1);
		CHECK(reader.ReadRecord() == nullptr);
	}

	remove_file(path);
}

TEST_CASE("FixedDecimal field type", "[fields][fixeddecimal]")
{
	auto path = temp_path("test_fixeddecimal");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Dec"), SRC::E_FT_FixedDecimal, 10, 2));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_fixeddecimal.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		rec->Reset();
		info[0]->SetFromDouble(rec.Get(), 123.45);
		writer.AppendRecord(rec->GetRecord());
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_fixeddecimal.yxdb"));

		const SRC::RecordData* rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);

		auto val = reader.m_recordInfo[0]->GetAsDouble(rec);
		CHECK_FALSE(val.bIsNull);
		CHECK_THAT(val.value, Catch::Matchers::WithinRel(123.45, 1e-6));
	}

	remove_file(path);
}

TEST_CASE("Blob field type roundtrip", "[fields][blob]")
{
	auto path = temp_path("test_blob");

	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("Data"), SRC::E_FT_Blob, 1000));

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB writer;
		writer.Create(U16("test_blob.yxdb"), info.GetRecordXmlMetaData());

		auto rec = info.CreateRecord();
		rec->Reset();

		// Write some binary data
		const unsigned char blob_data[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD};
		SRC::BlobVal bv;
		bv.pValue = blob_data;
		bv.nLength = sizeof(blob_data);
		info[0]->SetFromBlob(rec.Get(), bv);
		writer.AppendRecord(rec->GetRecord());
		writer.Close();
	}

	{
		Alteryx::OpenYXDB::Open_AlteryxYXDB reader;
		reader.Open(U16("test_blob.yxdb"));

		const SRC::RecordData* rec = reader.ReadRecord();
		REQUIRE(rec != nullptr);

		auto val = reader.m_recordInfo[0]->GetAsBlob(rec);
		CHECK_FALSE(val.bIsNull);
		REQUIRE(val.value.nLength == 6);

		auto* p = static_cast<const unsigned char*>(val.value.pValue);
		CHECK(p[0] == 0x00);
		CHECK(p[1] == 0x01);
		CHECK(p[2] == 0x02);
		CHECK(p[3] == 0xFF);
		CHECK(p[4] == 0xFE);
		CHECK(p[5] == 0xFD);
	}

	remove_file(path);
}

TEST_CASE("RecordInfo metadata XML is valid", "[metadata]")
{
	SRC::RecordInfo info;
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("A"), SRC::E_FT_Int32));
	info.AddField(SRC::RecordInfo::CreateFieldXml(U16("B"), SRC::E_FT_V_String, 256));

	auto xml = info.GetRecordXmlMetaData();
	auto xmlStr = SRC::ConvertToAString(xml.c_str());

	CHECK(xmlStr.find("RecordInfo") != std::string::npos);
	CHECK(xmlStr.find("\"A\"") != std::string::npos);
	CHECK(xmlStr.find("\"B\"") != std::string::npos);
	CHECK(xmlStr.find("Int32") != std::string::npos);
	CHECK(xmlStr.find("V_String") != std::string::npos);
}

TEST_CASE("Field type predicates", "[fieldtype]")
{
	CHECK(SRC::IsString(SRC::E_FT_String));
	CHECK(SRC::IsString(SRC::E_FT_WString));
	CHECK(SRC::IsString(SRC::E_FT_V_String));
	CHECK(SRC::IsString(SRC::E_FT_V_WString));
	CHECK_FALSE(SRC::IsString(SRC::E_FT_Int32));
	CHECK_FALSE(SRC::IsString(SRC::E_FT_Double));

	CHECK(SRC::IsNumeric(SRC::E_FT_Int32));
	CHECK(SRC::IsNumeric(SRC::E_FT_Double));
	CHECK(SRC::IsNumeric(SRC::E_FT_Float));
	CHECK(SRC::IsNumeric(SRC::E_FT_Int64));
	CHECK_FALSE(SRC::IsNumeric(SRC::E_FT_String));

	CHECK(SRC::IsDate(SRC::E_FT_Date));
	CHECK(SRC::IsDate(SRC::E_FT_DateTime));
	CHECK_FALSE(SRC::IsDate(SRC::E_FT_Int32));

	CHECK(SRC::IsBinary(SRC::E_FT_Blob));
	CHECK(SRC::IsBinary(SRC::E_FT_SpatialObj));
	CHECK_FALSE(SRC::IsBinary(SRC::E_FT_String));
}
