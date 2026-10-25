// Copyright(C) 2005 - 2019 Alteryx, Inc. All rights reserved.
// This file is distributed to Alteryx customers as part of the
// Software Development Kit.

// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=56549
// include guards prevent redefiniton errors while building e2
#ifndef XSRCLIB_RECORDLIB_INCLUDE_RECORDLIB_RECORDINFO_H
#define XSRCLIB_RECORDLIB_INCLUDE_RECORDLIB_RECORDINFO_H

#include <map>

#include "Base/MiniXmlParser.h"

#include "FieldBase.h"
#include "RecordLibExport.h"
#ifndef SRCLIB_REPLACEMENT
	#include <Base/Glot.h>
#endif

#include "Record.h"
#include "RecordData.h"

namespace SRC {
///////////////////////////////////////////////////////////////////////////////
//	class RecordInfo
class RecordInfo
{
	typedef std::vector<SmartPointerRefObj<FieldBase>> T_FieldVector;

	int m_nFixedRecordSize = 0;
	bool m_bContainsVarData = false;
	bool m_bLockIn = false;
	bool m_bStrictNaming;
	T_FieldVector m_vFields;

	std::vector<StringNoCase> m_vOriginalFieldNames;

	// Fast field name hash
	struct FieldNameUniquify
	{
		unsigned m_nHash;
		StringNoCase m_strFieldName;

		void ComputeHash();

		FieldNameUniquify(StringNoCase strFieldName);

		FieldNameUniquify& operator=(StringNoCase strFieldName);

		bool operator<(const FieldNameUniquify& rhs) const;
	};

	std::map<FieldNameUniquify, unsigned> m_mapFieldNums;

	unsigned m_nMaxFieldLen;
	const GenericEngineBase* m_pGenericEngineBase;
	StringNoCase ValidateFieldName(StringNoCase strFieldName, unsigned nFieldNum, bool bIssueWarnings = true);

	static const int MaxFieldsLimit = 32000;

public:
	RECORDLIB_EXPORT_CPP RecordInfo(
		unsigned nMaxFieldLen = 255,
		bool bStrictNaming = false,
		const GenericEngineBase* pGenericEngineBase = NULL);

	// this will set the LockIn flag when producing the RecordXML and accept it when reading
	RECORDLIB_EXPORT_CPP void SetLockIn(bool bLockIn = true);

	RECORDLIB_EXPORT_CPP RecordInfo(RecordInfo&& o);
	RECORDLIB_EXPORT_CPP RecordInfo(const RecordInfo& o);
	RECORDLIB_EXPORT_CPP RecordInfo& operator=(const RecordInfo& o);
	RECORDLIB_EXPORT_CPP RecordInfo& operator=(RecordInfo&& o);

	RECORDLIB_EXPORT_CPP void SetGenericEngine(const GenericEngineBase* pGenericEngineBase);
	RECORDLIB_EXPORT_CPP const GenericEngineBase* GetGenericEngine() const;

	RECORDLIB_EXPORT_CPP unsigned NumFields() const;
	RECORDLIB_EXPORT_CPP const FieldBase* operator[](size_t n) const;
	RECORDLIB_EXPORT_CPP void ResetForLateRename(unsigned maxlen, bool bStrictNaming);

	RECORDLIB_EXPORT_CPP void SwapFieldNames(int nField1, int nField2);

	RECORDLIB_EXPORT_CPP const FieldBase* AddField(const FieldSchema& fieldSchema, bool bIssueWarnings = true);
	RECORDLIB_EXPORT_CPP void AddField(SmartPointerRefObj<FieldBase> pField, bool bIssueWarnings = true);
	//Following functions will be removed after all cases will use AddField(const FieldSchema&) overload
	RECORDLIB_EXPORT_CPP const FieldBase* AddField(
		const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& tagField,
		const String::TChar* pNamePrefix = NULL,
		bool bIssueWarnings = true);
	RECORDLIB_EXPORT_CPP const FieldBase* AddField(
		StringNoCase strFieldName,
		E_FieldType ft,
		int nSize = 0,
		int nScale = 0,
		String strSource = String(),
		String strDescription = String(),
		bool bIssueWarnings = true);

	// this might give your suggestion of a name a number at the end if there is a conflict
	RECORDLIB_EXPORT_CPP const FieldBase* RenameField(unsigned nField, StringNoCase strNewName);
	RECORDLIB_EXPORT_CPP const FieldBase* RenameField(StringNoCase strOldFieldName, StringNoCase strNewFieldName);
	/** Rename fields oldFieldNames to newFieldNames without conflicting with not yet renamed fields 
		@param  oldFieldNames		Fields to find
		@param  newFieldNames		Strings to rename fields to */
	RECORDLIB_EXPORT_CPP void RenameFields(
		const std::vector<StringNoCase>& oldFieldNames,
		const std::vector<StringNoCase>& newFieldNames);

	static RECORDLIB_EXPORT_CPP String CreateFieldXml(
		String strFieldName,
		E_FieldType ft,
		unsigned nSize = 0,
		int nScale = 0,
		String strSource = String(),
		String strDescription = String());
	static RECORDLIB_EXPORT_CPP String GetFieldXml(
		const FieldSchema& field,
		const String::TChar* pFieldNamePrefix = _U(""),
		const String::TChar* pFieldNameSuffix = _U(""),
		bool bIncludeSource = true);

	RECORDLIB_EXPORT_CPP String GetRecordXmlMetaData(bool bIncludeSource = true) const;

	RECORDLIB_EXPORT_CPP void InitFromXml(
		const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& xmlTag,
		const String::TChar* pNamePrefix = NULL,
		bool bIgnoreLockIn = false);

	// The field names exactly as written in a RecordInfo XML document, before
	// any renaming that InitFromXml applies to make them unique or valid.
	static RECORDLIB_EXPORT_CPP std::vector<String> FieldNamesInXml(
		const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& xmlTag);

	E1ONLY_RECORDLIB_EXPORT_CPP SmartPointerRefObj<Record> CreateRecord() const;

	RECORDLIB_EXPORT_CPP int GetFieldNum(StringNoCase strField, bool bThrowError = true) const;
	RECORDLIB_EXPORT_CPP const FieldBase* GetFieldByName(StringNoCase strField, bool bThrowError = true) const;

	/*
		* GetFieldAndIndexByType
		* Find first field value by wanted field type,
		* if not found, and pi_bThrowOnError is true, 
		* throws an exception, otherwise returns <0, NULL>
		* if found, returns pair<idx, pFieldBase *>
		*/
	RECORDLIB_EXPORT_CPP std::pair<unsigned, const SRC::FieldBase*> GetFieldAndIndexByType(
		const SRC::E_FieldType pi_FieldType,
		const bool pi_bThrowOnError = false,
		unsigned nSkipToIndex = 0) const;

	/*
		* GetFieldsByType
		* Find all by wanted field type,
		*
		*  returns vector< pFieldBase *>
		*/
	RECORDLIB_EXPORT_CPP std::vector<const SRC::FieldBase*> GetFieldsByType(const SRC::E_FieldType pi_FieldType) const;

	/*
		* GetFieldNumByType
		* Returns index of first field, which is wanted type.
		* If not found, throws an exception.
		* pi_bThrowOnError is false, then returns 0 in this case
		*/
	RECORDLIB_EXPORT_CPP unsigned long GetFieldNumByType(
		const SRC::E_FieldType pi_FieldType,
		const bool pi_bThrowOnError = true) const;

	/*
		* GetFieldByType
		* Returns first FieldBase *ptr, which is wanted type.
		* If not found, returns NULL or throws an exception if 
		* pi_bThrowOnError is true
		*/
	RECORDLIB_EXPORT_CPP const SRC::FieldBase* GetFieldByType(
		const SRC::E_FieldType pi_FieldType,
		const bool pi_bThrowOnError = false) const;

	RECORDLIB_EXPORT_CPP unsigned GetNumFieldsByType(const SRC::E_FieldType pi_FieldType) const;

	// returns true if type, name, scale, size are equal
	// if bCompareId is true - also compares id
	RECORDLIB_EXPORT_CPP bool CompareSchemas(const RecordInfo& o) const;

	RECORDLIB_EXPORT_CPP bool operator==(const RecordInfo& o) const;

	// returns true if everything about the fields are the same
	// other than the names can be different
	RECORDLIB_EXPORT_CPP bool EqualTypes(const RecordInfo& o, bool bAllowAdditionalFields = false) const;

#if !defined(SRCLIB_REPLACEMENT) && !defined(E2)
	// this returns a #.  If 2 records sets have the same # they should be binary compatible.
	RECORDLIB_EXPORT_CPP unsigned GetHash();
#endif
	// returns nLen, pValue
	RECORDLIB_EXPORT_CPP static BlobVal GetVarDataValue(const RecordData* pRec, int nFieldOffset);

	RECORDLIB_EXPORT_CPP static void SetVarDataValue(Record* pRec, int nFieldOffset, unsigned nLen, const void* pValue);

	E1ONLY_RECORDLIB_EXPORT_CPP size_t GetRecordLen(const RecordData* pSrc) const;
	RECORDLIB_EXPORT_CPP void AssertValid(const RecordData* pSrc) const;

	// copies a whole record to a memory buffer.
	// returns 0 if not enough room, numBytesUsed on success
	RECORDLIB_EXPORT_CPP size_t Copy(void* pDest, size_t nAvailibleBytes, const RecordData* pSrc) const;

	// copys a whole record with an identical structure.  Use RecordCopier for more flexablility
	RECORDLIB_EXPORT_CPP void Copy(Record* r_pRecordDest, const RecordData* pRecordSrc) const;

	static RECORDLIB_EXPORT_CPP int GetMaxFieldsLimit();

	// return is the record version #
	// 0 - pre 9.0
	// 1 - 9.0 (>256MB records)
	template <class TFile>
	unsigned Write(TFile& file, const RecordData* pRecord) const;
	template <class TFile>
	void Read(TFile& file, Record* r_pRecord) const;

	// Tell if a complete RecordData could be at this place in memory
	RECORDLIB_EXPORT_CPP bool HasWholeRecord(const void* pstart, size_t len) const;

	// The vector holds smart pointers, so when making a range based FOR it is
	// important to write it as:   for (const auto & fldPtr : recordInfo)
	// (note that little ampersand) otherwise you will have atomic increments and
	// decrements as you go around the loop, completely killing your performance.
	// Within the loop you can write fldPtr->Member() just as if you had
	// gotten the bare pointer from operator[] on the recordInfo
	RECORDLIB_EXPORT_CPP decltype(m_vFields.cbegin()) begin() const;
	// Kept end() inline for compiler to possibly optimize loops
	decltype(m_vFields.cend()) end() const
	{
		return m_vFields.cend();
	}

	// The vector holds smart pointers, so when making a range based FOR it is
	// important to write it as:   for (auto & fldPtr : recordInfo)
	// (note that little ampersand) otherwise you will have atomic increments and
	// decrements as you go around the loop, completely killing your performance.
	// Within the loop you can write fldPtr->Member() just as if you had
	// gotten the bare pointer from operator[] on the recordInfo
	RECORDLIB_EXPORT_CPP decltype(m_vFields.begin()) begin();
	// Kept end() inline for compiler to possibly optimize loops
	decltype(m_vFields.end()) end()
	{
		return m_vFields.end();
	}

	RECORDLIB_EXPORT_CPP int GetFixedRecordSize() const;
	RECORDLIB_EXPORT_CPP bool ContainsVarData() const;
};

// return is the record version #
// 0 - pre 9.0
// 1 - 9.0 (>256MB records)
template <class TFile>
unsigned RecordInfo::Write(TFile& file, const RecordData* pRecord) const
{
	int nWriteSize = m_nFixedRecordSize;
	if (m_bContainsVarData)
		nWriteSize += sizeof(int);
	file.Write(pRecord, nWriteSize);
	if (m_bContainsVarData)
	{
		int nVarDataSize;
		memcpy(&nVarDataSize, ToCharP(pRecord) + m_nFixedRecordSize, sizeof(int));
		file.Write(ToCharP(pRecord) + m_nFixedRecordSize + sizeof(int), nVarDataSize);

#ifndef __GNUG__
		static_assert(MaxFieldLength32 == 0x0fffffff, "MaxFieldLength32==0x0fffffff");
#endif
		return (nWriteSize + nVarDataSize > 0x0fffffff) ? 1 : 0;
	}
	return 0;
}

template <class TFile>
void RecordInfo::Read(TFile& file, Record* r_pRecord) const
{
	r_pRecord->Reset();

	int nReadSize = m_nFixedRecordSize;
	if (m_bContainsVarData)
		nReadSize += sizeof(int);
	file.Read(r_pRecord->m_pRecord, nReadSize);
	if (m_bContainsVarData)
	{
		int nVarDataSize;
		memcpy(&nVarDataSize, static_cast<char*>(r_pRecord->m_pRecord) + m_nFixedRecordSize, sizeof(int));
		assert(nVarDataSize >= 0);
		r_pRecord->Allocate(nVarDataSize + 4);
		if (nVarDataSize > 0)
			file.Read(static_cast<char*>(r_pRecord->m_pRecord) + m_nFixedRecordSize + sizeof(int), nVarDataSize);
		r_pRecord->m_bVarDataLenUnset = false;
	}
}
}  // namespace SRC

#endif /* XSRCLIB_RECORDLIB_INCLUDE_RECORDLIB_RECORDINFO_H */
