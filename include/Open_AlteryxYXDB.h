///////////////////////////////////////////////////////////////////////////////
//
// (C) 2005 SRC, LLC  -   All rights reserved
//
///////////////////////////////////////////////////////////////////////////////
//
// Module: Open_AlteryxYXDB.H
//
///////////////////////////////////////////////////////////////////////////////

#pragma once

#include <ctime>
#include <memory>

#include "Base/lzf_src.h"
#include "RecordLib/RecordInfo.h"

#if defined(__linux__) || defined(__APPLE__) || defined(BUILDING_OPEN_ALTERYX)
	#define OPEN_ALTERYX_EXPORT
#elif defined(OPEN_ALTERYX_EXPORTS)
	#define OPEN_ALTERYX_EXPORT __declspec(dllexport)
#else
	#define OPEN_ALTERYX_EXPORT __declspec(dllimport)
#endif

namespace Alteryx { namespace OpenYXDB {
using namespace SRC;

///////////////////////////////////////////////////////////////////////////////
// class File_Large
//
class OPEN_ALTERYX_EXPORT File_Large
{
	WString m_strFile;
	int m_iFileDescriptor;

public:
	File_Large();
	~File_Large();
	void Close();

	void OpenForRead(WString strFile);
	void OpenForWrite(WString strFile);

	inline WString GetFileName() const
	{
		return m_strFile;
	}
	inline bool IsOpen() const
	{
		return m_iFileDescriptor != -1;
	}

	int64_t Tell() const;

	void LSeek(int64_t nPos);

	unsigned Read(void* _pBuffer, unsigned nNumBytesToRead);

	unsigned Write(const void* _pBuffer, unsigned nNumBytesToWrite);

	static void GetAndThrowError(WString strErrorIntro);
};

const int RecordsPerBlock = 0x10000;
const int32_t ID_WRIGLEYDB = 0x00440205;
const int32_t ID_WRIGLEYDB_NoSpatialIndex = 0x00440204;
const int HeaderPageSize = 512;

struct OPEN_ALTERYX_EXPORT FileHeaderStruct
{
	char fileDesc[64];
	int32_t fileID;  // a long value unique to each file and version
	int32_t creationDate;
	int bitbucket1;
	int bitbucket2;
};

struct OPEN_ALTERYX_EXPORT HeaderData
{
	unsigned
		nMetaInfoLen;  // the MetaInfo XML immediatly follows the header.  It is a wide string, so it 2X this number of bytes
	int64_t nSpatialIndexPos;
	int64_t nRecordBlockIndexPos;
	int64_t nNumRecords;
	int nCompressionVersion;
};

struct OPEN_ALTERYX_EXPORT Header : public FileHeaderStruct
{
	HeaderData userHdr;

	char bitBucket[HeaderPageSize - sizeof(FileHeaderStruct) - sizeof(HeaderData)];

	inline Header()
	{
		memset(this, 0, sizeof(*this));
	}

	template <class T_File>
	inline void Write(T_File& outFile)
	{
		fileID = ID_WRIGLEYDB_NoSpatialIndex;
		time_t tTemp;
		time(&tTemp);
		creationDate = long(tTemp);

		strcpy(fileDesc, "Alteryx Database File");
		outFile.Write(this, sizeof(*this));
	}

	template <class T_File>
	inline void Read(T_File& inFile)
	{
		inFile.Read(this, sizeof(*this));
		CheckFileID(inFile);
	}

protected:
	template <class T_File>
	inline void CheckFileID(T_File& outFile)
	{
		if ((fileID & 0x00ff0000) != (ID_WRIGLEYDB & 0x00ff0000))
		{
			//Rprintf("Throwing an Error - The FileID does not match in the FileHeader.\n");
			throw Error(outFile.GetFileName() + U16(" \nThe FileID does not match in the FileHeader."));
		}

		unsigned nFileVersion = fileID & 0xff;
		unsigned nMaxVersion = ID_WRIGLEYDB & 0xff;
		unsigned nMinVersion = (ID_WRIGLEYDB >> 8) & 0xff;
		if (nMinVersion == 0)
			nMinVersion = nMaxVersion;
		if (nFileVersion < nMinVersion)
		{
			//Rprintf("Throwing an Error - The file version is older than expected.  This file cannot be read.\n");
			throw Error(
				outFile.GetFileName() + U16(" \nThe file version is older than expected.  This file cannot be read."));
		}
		if (nFileVersion > nMaxVersion)
		{
			//Rprintf("Throwing an Error - The file version is newer than expected.  This file cannot be read.\n");
			throw Error(
				outFile.GetFileName() + U16(" \nThe file version is newer than expected.  This file cannot be read."));
		}
	}
};

///////////////////////////////////////////////////////////////////////////////
// Open_AlteryxYXDB
class OPEN_ALTERYX_EXPORT Open_AlteryxYXDB
{
	std::unique_ptr<File_Large> m_pFile;

public:
	RecordInfo m_recordInfo;

private:
	SmartPointerRefObj<Record> m_pRecord;

	bool m_bIndexStartsBlock;

	void GoBlockRecord(int64_t nRecord);

	std::unique_ptr<LZFBufferedInput<File_Large*>> m_pCompressInput;
	std::unique_ptr<LZFBufferedOutput<File_Large*, GenericEngineBase>> m_pCompressOutput;

	Header m_header;
	bool m_bCreateMode;

	// this is always the # of the next record to be read
	int64_t m_nCurrentRecord;

	// the record blocks are always 64K records, except for the last one
	std::vector<int64_t> m_vRecordBlockIndexPos;

public:
	Open_AlteryxYXDB()
		: m_bIndexStartsBlock(false)
		, m_bCreateMode(false)
		, m_nCurrentRecord(0)
	{
	}
	~Open_AlteryxYXDB();
	void Close();

	void Open(WString strFile);
	void Create(WString strFile, const U16unit* pRecordInfoXml);

	const RecordData* ReadRecord();
	void AppendRecord(const RecordData* pRec);

	int64_t GetNumRecords();

	void GoRecord(int64_t nRecord = 0);

	WString GetRecordXmlMetaData();
};
}}  // namespace Alteryx::OpenYXDB
