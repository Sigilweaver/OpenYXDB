
#include "stdafx.h"

#include "Open_AlteryxYXDB.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

#include <fcntl.h>  // for open, error codes
#include <sys/stat.h>

#ifdef __GNUG__
	#include <unistd.h>  // for close
	#ifndef O_BINARY
		#define O_BINARY 0
	#endif
#else
	#include <io.h>
	#define _LARGEFILE_SOURCE
	#define _LARGEFILE64_SOURCE
#endif

namespace Alteryx { namespace OpenYXDB {

#ifdef __GNUG__
// Convert UTF-16 (WString) to UTF-8 for Linux filesystem paths.
// ConvertToAString uses Latin-1 which breaks non-ASCII filenames on Linux
// since the filesystem expects UTF-8.
static AString WStringToUtf8Path(const WString& ws) {
	AString result;
	const U16unit* s = ws.c_str();
	while (*s) {
		uint32_t cp = static_cast<uint16_t>(*s);
		// Handle surrogate pairs
		if (cp >= 0xD800 && cp <= 0xDBFF) {
			uint16_t trail = static_cast<uint16_t>(*(s + 1));
			if (trail >= 0xDC00 && trail <= 0xDFFF) {
				cp = 0x10000 + ((cp - 0xD800) << 10) + (trail - 0xDC00);
				++s;
			}
		}
		if (cp < 0x80) {
			result.push_back(static_cast<char>(cp));
		} else if (cp < 0x800) {
			result.push_back(static_cast<char>(0xC0 | (cp >> 6)));
			result.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		} else if (cp < 0x10000) {
			result.push_back(static_cast<char>(0xE0 | (cp >> 12)));
			result.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
			result.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		} else {
			result.push_back(static_cast<char>(0xF0 | (cp >> 18)));
			result.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
			result.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
			result.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
		}
		++s;
	}
	return result;
}
#endif

File_Large::File_Large()
	: m_iFileDescriptor(-1)
{
}

File_Large::~File_Large()
{
	Close();
}

void File_Large::Close()
{
	if (m_iFileDescriptor != -1)
	{
#ifdef __GNUG__
		close(m_iFileDescriptor);
#else
		_close(m_iFileDescriptor);
#endif
		m_iFileDescriptor = -1;
	}
}

// Flush file data to stable storage so a write error the system defers until
// now is reported here and not lost.
void File_Large::Sync()
{
#ifdef __GNUG__
	if (fsync(m_iFileDescriptor) != 0)
#else
	if (_commit(m_iFileDescriptor) != 0)
#endif
		ThrowOsError(U16("Error in Sync: "));
}

// Like Close(), but reports a failure of close(2) itself.
void File_Large::CloseChecked()
{
	if (m_iFileDescriptor == -1)
		return;
	int fd = m_iFileDescriptor;
	m_iFileDescriptor = -1;
#ifdef __GNUG__
	int rc = close(fd);
#else
	int rc = _close(fd);
#endif
	if (rc != 0)
	{
		int nError = errno;
		WString errorMsg;
		ConvertString(errorMsg, strerror(nError));
		errorMsg.Trim();
		Error e(ErrorKind::IO, U16("Error in Close: ") + errorMsg);
		e.SetOsError(nError, m_strFile);
		throw e;
	}
}

void File_Large::OpenForRead(WString strFile)
{
	m_strFile = strFile;

#ifdef __GNUG__
	AString astrFile = WStringToUtf8Path(strFile);
	m_iFileDescriptor = open(astrFile.c_str(), O_RDONLY | O_BINARY, 0);
#else
	m_iFileDescriptor = _wopen(strFile, _O_RDONLY | _O_BINARY);
#endif
	if (m_iFileDescriptor == -1)
	{
		ThrowOsError(U16("Error in OpenForRead: "));
	}
}

void File_Large::OpenForWrite(WString strFile)
{
	m_strFile = strFile;

#ifdef __GNUG__
	AString astrFile = WStringToUtf8Path(strFile);
	m_iFileDescriptor = open(astrFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
#else
	m_iFileDescriptor = _wopen(strFile, _O_RDWR | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
#endif

	if (m_iFileDescriptor == -1)
		ThrowOsError(U16("Error in OpenForWrite: "));
}

int64_t File_Large::Tell() const
{
	int64_t seekPos = 0;

#ifdef __GNUG__
	seekPos = lseek(m_iFileDescriptor, 0, SEEK_CUR);
#else
	seekPos = _lseeki64(m_iFileDescriptor, 0, SEEK_CUR);
#endif
	if (seekPos == -1)
		ThrowOsError(U16("Error in Tell: "));

	return seekPos;
}

int64_t File_Large::Size() const
{
#ifdef __GNUG__
	struct stat st;
	if (fstat(m_iFileDescriptor, &st) != 0)
		File_Large::GetAndThrowError(U16("Error in Size: "));
	return static_cast<int64_t>(st.st_size);
#else
	int64_t size = _filelengthi64(m_iFileDescriptor);
	if (size < 0)
		File_Large::GetAndThrowError(U16("Error in Size: "));
	return size;
#endif
}

void File_Large::LSeek(int64_t nPos)
{
	int64_t seekPos = 0;

#ifdef __GNUG__
	seekPos = lseek(m_iFileDescriptor, nPos, SEEK_SET);
#else
	seekPos = _lseeki64(m_iFileDescriptor, nPos, SEEK_SET);
#endif
	if (seekPos == -1)
		ThrowOsError(U16("Error in LSeek: "));
}

unsigned File_Large::Read(void* _pBuffer, unsigned nNumBytesToRead)
{
	int64_t nStartPos = Tell();

#ifdef __GNUG__
	ssize_t ret = read(m_iFileDescriptor, _pBuffer, nNumBytesToRead);
#else
	int ret = _read(m_iFileDescriptor, _pBuffer, nNumBytesToRead);
#endif

	if (ret < 0)
		ThrowOsError(U16("Error in Read: "));

	if (static_cast<unsigned>(ret) != nNumBytesToRead)
	{
		// The file ended before the content it declares: a malformed file, not
		// an OS error.
		std::string msg = "Error in Read: expected " + std::to_string(nNumBytesToRead) + " bytes at offset "
			+ std::to_string(nStartPos) + " but got " + std::to_string(ret) + " (unexpected end of file)";
		throw Error(ErrorKind::Format, AString(msg.c_str()));
	}

	return static_cast<unsigned>(ret);
}

unsigned File_Large::Write(const void* _pBuffer, unsigned nNumBytesToWrite)
{
	unsigned ret = 0;
#ifdef __GNUG__
	ret = write(m_iFileDescriptor, _pBuffer, nNumBytesToWrite);
#else
	ret = _write(m_iFileDescriptor, _pBuffer, nNumBytesToWrite);
#endif
	if (ret != nNumBytesToWrite)
		ThrowOsError(U16("Error in Write: "));

	return ret;
}

/*static*/ void File_Large::GetAndThrowError(WString strErrorIntro)
{
	int nError = errno;

	WString errorMsg;
	ConvertString(errorMsg, strerror(nError));
	errorMsg.Trim();
	throw Error(ErrorKind::IO, strErrorIntro + errorMsg);
}

void File_Large::ThrowOsError(WString strErrorIntro) const
{
	int nError = errno;

	WString errorMsg;
	ConvertString(errorMsg, strerror(nError));
	errorMsg.Trim();
	Error e(ErrorKind::IO, strErrorIntro + errorMsg);
	e.SetOsError(nError, m_strFile);
	throw e;
}

Open_AlteryxYXDB::~Open_AlteryxYXDB()
{
	try
	{
		Close();
	}
	catch (...)
	{
		// A destructor cannot throw. Callers who need to know whether the final
		// write succeeded must call Close() themselves.
		fprintf(stderr, "OpenYXDB: an error occurred while closing a file in a destructor\n");
	}
}

/*virtual*/ void Open_AlteryxYXDB::Close()
{
	if (m_pFile && m_pFile->IsOpen())
	{
		if (m_bCreateMode)
		{
			m_pCompressOutput->FlushBuffer();
			m_header.userHdr.nNumRecords = m_nCurrentRecord;

			m_header.userHdr.nRecordBlockIndexPos = m_pFile->Tell();
			m_header.userHdr.nCompressionVersion = 1;

			// Write the block index so readers can seek to arbitrary blocks
			unsigned nIndexSize = static_cast<unsigned>(m_vRecordBlockIndexPos.size());
			m_pFile->Write(&nIndexSize, sizeof(nIndexSize));
			if (nIndexSize > 0)
				m_pFile->Write(&m_vRecordBlockIndexPos[0], nIndexSize * sizeof(int64_t));

			m_pFile->LSeek(0);
			m_pFile->Write(&m_header, sizeof(m_header));
			m_pFile->Sync();
			m_pFile->CloseChecked();
		}
		else
			m_pFile->Close();

		m_pFile.reset();
	}
}

/*virtual*/ void Open_AlteryxYXDB::Create(WString strFile, const U16unit* pRecordInfoXml)
{
	m_pFile.reset(new File_Large());
	m_pFile->OpenForWrite(strFile);

	m_bCreateMode = true;

	m_header.userHdr.nMetaInfoLen = TStrLen(pRecordInfoXml) + 1;  // +1 to write the NULL terminator for convenience
	m_header.Write(*m_pFile);
	m_pFile->Write(pRecordInfoXml, (m_header.userHdr.nMetaInfoLen) * sizeof(U16unit));

	m_pCompressOutput = std::make_unique<LZFBufferedOutput<File_Large*, GenericEngineBase>>(
		this->m_recordInfo.GetGenericEngine(), m_pFile.get());

	m_recordInfo.InitFromXml(pRecordInfoXml);
	m_pRecord = m_recordInfo.CreateRecord();
}

/*virtual*/ void Open_AlteryxYXDB::AppendRecord(const RecordData* pRec)
{
	if ((m_nCurrentRecord % RecordsPerBlock) == 0)
	{
		m_pCompressOutput->FlushBuffer();
		m_vRecordBlockIndexPos.push_back(m_pFile->Tell());
	}

	m_recordInfo.Write(*m_pCompressOutput, pRec);
	m_nCurrentRecord++;
}

[[noreturn]] static void ThrowFormatError(const std::string& msg)
{
	throw Error(ErrorKind::Format, AString(msg.c_str()));
}

void Open_AlteryxYXDB::ValidateHeader()
{
	static const char kMagic[] = "Alteryx Database File";
	const HeaderData& u = m_header.userHdr;

	if (strncmp(m_header.fileDesc, kMagic, sizeof(kMagic) - 1) != 0)
		ThrowFormatError(std::string("Not a YXDB file: header does not start with \"Alteryx Database File\""));

	// Only file versions 4 and 5 are known to exist; 2 and 3 have no known
	// writer and their record index layout is not handled here.
	unsigned nVersion = static_cast<unsigned>(m_header.fileID) & 0xff;
	if (nVersion != 4 && nVersion != 5)
		ThrowFormatError(std::string("Unsupported YXDB file version ") + std::to_string(nVersion) + ", expected 4 or 5");

	if (u.nCompressionVersion != 0 && u.nCompressionVersion != 1)
		ThrowFormatError(
			std::string("Unsupported YXDB compression version ") + std::to_string(u.nCompressionVersion)
			+ ", expected 0 or 1");

	// Report an impossible schema length before the finalization check below,
	// which would otherwise blame a header that was never written.
	{
		const int64_t nFileSizeNow = m_pFile->Size();
		if (u.nMetaInfoLen == 0
			|| int64_t(sizeof(m_header)) + int64_t(u.nMetaInfoLen) * int64_t(sizeof(U16unit)) > nFileSizeNow)
			ThrowFormatError(
				"Corrupt file: schema length " + std::to_string(u.nMetaInfoLen)
				+ " characters does not fit in a file of " + std::to_string(nFileSizeNow) + " bytes");
	}

	if (u.nNumRecords < 0)
		ThrowFormatError(std::string("Invalid YXDB record count ") + std::to_string(u.nNumRecords));

	// The spatial index position is zero in version 4 files; in version 5 files
	// it points into the file between the end of the schema and the block index.
	int64_t nDataStartForSpatial = int64_t(sizeof(m_header)) + int64_t(u.nMetaInfoLen) * int64_t(sizeof(U16unit));
	if (nVersion == 4 && u.nSpatialIndexPos != 0)
		ThrowFormatError(
			"Corrupt file: spatial index position " + std::to_string(u.nSpatialIndexPos)
			+ " in a file version that has no spatial index");
	if (nVersion == 5 && u.nSpatialIndexPos != 0
		&& (u.nSpatialIndexPos < nDataStartForSpatial || u.nSpatialIndexPos >= u.nRecordBlockIndexPos))
		ThrowFormatError(
			"Corrupt file: spatial index position " + std::to_string(u.nSpatialIndexPos)
			+ " is outside the record data");

	// The header is rewritten at Close(), so a file whose writer never closed
	// it still has a zeroed record count and index position.
	int64_t nDataStart = int64_t(sizeof(m_header)) + int64_t(u.nMetaInfoLen) * int64_t(sizeof(U16unit));
	if (u.nRecordBlockIndexPos < nDataStart)
		ThrowFormatError(
			std::string("YXDB file was not finalized: record block index position ")
			+ std::to_string(u.nRecordBlockIndexPos) + " is before the record data start "
			+ std::to_string(nDataStart));
}

void Open_AlteryxYXDB::ReadBlockIndex(int64_t nFileSize)
{
	const HeaderData& u = m_header.userHdr;
	const int64_t nDataStart = int64_t(sizeof(m_header)) + int64_t(u.nMetaInfoLen) * int64_t(sizeof(U16unit));
	// Written so that no intermediate value can overflow: a record count near
	// INT64_MAX must not wrap into a plausible block count.
	const int64_t nExpectedBlocks = u.nNumRecords / RecordsPerBlock + (u.nNumRecords % RecordsPerBlock != 0 ? 1 : 0);

	if (nFileSize < 4 || u.nRecordBlockIndexPos < nDataStart || u.nRecordBlockIndexPos > nFileSize - 4)
		ThrowFormatError(
			"Corrupt file: record block index position " + std::to_string(u.nRecordBlockIndexPos)
			+ " is outside the file");

	m_pFile->LSeek(u.nRecordBlockIndexPos);
	unsigned nCount = 0;
	m_pFile->Read(&nCount, sizeof(nCount));
	if (int64_t(nCount) != nExpectedBlocks)
		ThrowFormatError(
			"Corrupt file: record block index has " + std::to_string(nCount) + " entries but "
			+ std::to_string(u.nNumRecords) + " records need " + std::to_string(nExpectedBlocks));
	const int64_t nIndexBytes = int64_t(nCount) * 8;
	if (nIndexBytes > nFileSize - u.nRecordBlockIndexPos - 4)
		ThrowFormatError("Corrupt file: record block index extends past the end of the file");
	// Nothing follows the block index in any known file.
	const int64_t nTrailing = nFileSize - u.nRecordBlockIndexPos - 4 - nIndexBytes;
	if (nTrailing != 0)
		ThrowFormatError(
			"Corrupt file: " + std::to_string(nTrailing) + " bytes follow the record block index");

	m_vRecordBlockIndexPos.assign(nCount, 0);
	if (nCount > 0)
		m_pFile->Read(m_vRecordBlockIndexPos.data(), nCount * sizeof(int64_t));

	int64_t nPrev = -1;
	for (unsigned i = 0; i < nCount; ++i)
	{
		int64_t e = m_vRecordBlockIndexPos[i];
		if (e < nDataStart || e >= u.nRecordBlockIndexPos || e <= nPrev)
			ThrowFormatError(
				"Corrupt file: record block index entry " + std::to_string(i) + " (" + std::to_string(e)
				+ ") is out of range or out of order");
		nPrev = e;
	}
	if (nCount > 0 && m_vRecordBlockIndexPos[0] != nDataStart)
		ThrowFormatError("Corrupt file: first record block does not start where the schema ends");

	m_pFile->LSeek(nDataStart);
}

// When records are read in order, the byte stream must arrive exactly at the
// next block (or at the index after the last one) with no chunk bytes unused.
// Files with a spatial index interleave spatial index data after the record
// data of each block, so for those the stream may only end at or before the
// next block.
void Open_AlteryxYXDB::CheckStreamAtBlockBoundary(int64_t nExpectedPos, const char* pWhat)
{
	bool bDrained = !m_pCompressInput || m_pCompressInput->IsDrained();
	int64_t nPos = m_pFile->Tell();
	bool bHasSpatialIndex = m_header.userHdr.nSpatialIndexPos != 0;
	bool bPosOk = bHasSpatialIndex ? nPos <= nExpectedPos : nPos == nExpectedPos;
	if (!bDrained || !bPosOk)
		ThrowFormatError(
			std::string("Corrupt file: record data ") + pWhat + " ends at byte " + std::to_string(nPos)
			+ (bDrained ? "" : " with unread data in the last chunk") + ", expected byte "
			+ (bHasSpatialIndex ? "at most " : "") + std::to_string(nExpectedPos));
}

/*virtual*/ void Open_AlteryxYXDB::Open(WString strFile)
{
	m_pFile.reset(new File_Large());
	m_pFile->OpenForRead(strFile);

	m_header.Read(*m_pFile);

	ValidateHeader();

	m_bIndexStartsBlock = (m_header.fileID & 0xff) == 3;
	if (m_header.userHdr.nCompressionVersion == 1)
		m_pCompressInput = std::make_unique<LZFBufferedInput<File_Large*>>(m_pFile.get());

	const int64_t nFileSize = m_pFile->Size();

	// The metadata is a UTF-16 string that ends in a NUL; its length is in
	// characters, including that NUL.
	const unsigned nMetaChars = m_header.userHdr.nMetaInfoLen;
	if (nMetaChars == 0 || int64_t(sizeof(m_header)) + int64_t(nMetaChars) * int64_t(sizeof(U16unit)) > nFileSize)
		ThrowFormatError(
			std::string("Corrupt file: metadata length ") + std::to_string(nMetaChars)
			+ " characters does not fit in a file of " + std::to_string(nFileSize) + " bytes");

	std::vector<U16unit> vMeta(size_t(nMetaChars) + 1, 0);
	m_pFile->Read(vMeta.data(), nMetaChars * sizeof(U16unit));
	if (vMeta[nMetaChars - 1] != 0)
		ThrowFormatError("Corrupt file: schema XML is not NUL terminated");
	for (size_t i = 0; i + 1 < nMetaChars; ++i)
		if (vMeta[i] == 0)
			ThrowFormatError("Corrupt file: schema XML contains an embedded NUL at character " + std::to_string(i));
	String strRecordInfoXml(vMeta.data());

	m_strFileRecordInfoXml = strRecordInfoXml;
	m_recordInfo.InitFromXml(strRecordInfoXml);
	if (m_recordInfo.NumFields() == 0)
		throw Error(ErrorKind::Schema, AString("YXDB schema XML has no RecordInfo element or no Field elements"));
	m_pRecord = m_recordInfo.CreateRecord();

	// make sure we are at the first record in the file
	if (m_pFile->Tell() != int64_t(sizeof(m_header)) + int64_t(nMetaChars) * int64_t(sizeof(U16unit)))
		ThrowFormatError("Corrupt file: record data does not start where the schema ends");

	ReadBlockIndex(nFileSize);
	m_nStreamRecord = 0;
}

/*virtual*/ WString Open_AlteryxYXDB::GetRecordXmlMetaData()
{
	return m_recordInfo.GetRecordXmlMetaData();
}

/*virtual*/ const RecordData* Open_AlteryxYXDB::ReadRecord()
{
	if (m_nCurrentRecord == m_header.userHdr.nNumRecords)
		return NULL;

	if ((m_nCurrentRecord % RecordsPerBlock) == 0)
		GoBlockRecord(m_nCurrentRecord);

	m_nCurrentRecord++;
	Record* pRec = m_pRecord.Get();
	pRec->Reset();

	if (m_header.userHdr.nCompressionVersion == 1)
		m_recordInfo.Read(*m_pCompressInput, pRec);
	else
		m_recordInfo.Read(*m_pFile, pRec);
	m_nStreamRecord = m_nCurrentRecord;

	// the last record must end exactly where the block index starts
	if (m_nCurrentRecord == m_header.userHdr.nNumRecords)
		CheckStreamAtBlockBoundary(m_header.userHdr.nRecordBlockIndexPos, "of the last block");

	return pRec->GetRecord();
}

/*virtual*/ int64_t Open_AlteryxYXDB::GetNumRecords()
{
	return m_header.userHdr.nNumRecords;
}

void Open_AlteryxYXDB::GoBlockRecord(int64_t nRecord)
{
	if (nRecord == 0)
	{
		m_pFile->LSeek(sizeof(m_header) + m_header.userHdr.nMetaInfoLen * sizeof(U16unit));
		if (m_pCompressInput.get())
			m_pCompressInput->Reset();
		m_nCurrentRecord = 0;
	}
	else
	{
		const int64_t nBlock = nRecord / RecordsPerBlock;
		if (nBlock >= int64_t(m_vRecordBlockIndexPos.size()))
			ThrowFormatError("Corrupt file: record " + std::to_string(nRecord) + " has no record block index entry");
		int64_t nNewPos = m_vRecordBlockIndexPos[size_t(nBlock)];
		if (m_nStreamRecord == nRecord)
			CheckStreamAtBlockBoundary(nNewPos, ("of block " + std::to_string(nBlock - 1)).c_str());
		m_pFile->LSeek(nNewPos);
		if (m_pCompressInput.get())
			m_pCompressInput->Reset();
	}
	m_nStreamRecord = nRecord;
}

/*virtual*/ void Open_AlteryxYXDB::GoRecord(int64_t nRecord /*= 0*/)
{
	if (nRecord >= m_header.userHdr.nNumRecords || nRecord < 0)
		throw Error(U16("Open_AlteryxYXDB::GoRecord: Attempt to seek past the end of the file"));

	if (nRecord == m_nCurrentRecord)
		;  // do nothing
	else
	{
		unsigned numSkipRecs = 0;
		if (nRecord > m_nCurrentRecord
			&& nRecord < (m_nCurrentRecord + RecordsPerBlock - (m_nCurrentRecord % RecordsPerBlock)))
			numSkipRecs = unsigned(nRecord - m_nCurrentRecord);
		else
		{
			// read record will reset to the correct spot when it tries to read the next record
			numSkipRecs = unsigned(nRecord % RecordsPerBlock);
			m_nCurrentRecord = nRecord - numSkipRecs;
			m_nStreamRecord = -1;
		}

		for (unsigned x = 0; x < numSkipRecs; x++)
			ReadRecord();
	}
}

}}  // namespace Alteryx::OpenYXDB
