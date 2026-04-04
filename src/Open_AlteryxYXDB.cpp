
#include "stdafx.h"

#include "Open_AlteryxYXDB.h"

#include <cerrno>

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
		File_Large::GetAndThrowError(U16("Error in OpenForRead: "));
	}
}

void File_Large::OpenForWrite(WString strFile)
{
	m_strFile = strFile;

#ifdef __GNUG__
	AString astrFile = WStringToUtf8Path(strFile);
	m_iFileDescriptor = open(astrFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, S_IRUSR | S_IWUSR);
#else
	m_iFileDescriptor = _wopen(strFile, _O_RDWR | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
#endif

	if (m_iFileDescriptor == -1)
		File_Large::GetAndThrowError(U16("Error in OpenForWrite: "));
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
		File_Large::GetAndThrowError(U16("Error in Tell: "));

	return seekPos;
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
		File_Large::GetAndThrowError(U16("Error in LSeek: "));
}

unsigned File_Large::Read(void* _pBuffer, unsigned nNumBytesToRead)
{
	unsigned ret = 0;

#ifdef __GNUG__
	ret = read(m_iFileDescriptor, _pBuffer, nNumBytesToRead);
#else
	ret = _read(m_iFileDescriptor, _pBuffer, nNumBytesToRead);
#endif

	if (ret != nNumBytesToRead)
		File_Large::GetAndThrowError(U16("Error in Read: Unexpected number of bytes to read"));

	return ret;
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
		File_Large::GetAndThrowError(U16("Error in Write: "));

	return ret;
}

/*static*/ void File_Large::GetAndThrowError(WString strErrorIntro)
{
	int nError = errno;

	WString errorMsg;
	ConvertString(errorMsg, strerror(nError));
	errorMsg.Trim();
	throw Error(strErrorIntro + errorMsg);
}

Open_AlteryxYXDB::~Open_AlteryxYXDB()
{
	try
	{
		Close();
	}
	catch (...)
	{
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
			m_pFile->Close();
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

/*virtual*/ void Open_AlteryxYXDB::Open(WString strFile)
{
	m_pFile.reset(new File_Large());
	m_pFile->OpenForRead(strFile);

	m_header.Read(*m_pFile);

	m_bIndexStartsBlock = (m_header.fileID & 0xff) == 3;
	if (m_header.userHdr.nCompressionVersion == 1)
		m_pCompressInput = std::make_unique<LZFBufferedInput<File_Large*>>(m_pFile.get());

	String strRecordInfoXml;
	U16unit* pRecordInfoXml = strRecordInfoXml.Lock(m_header.userHdr.nMetaInfoLen);
	m_pFile->Read(pRecordInfoXml, m_header.userHdr.nMetaInfoLen * sizeof(U16unit));
	strRecordInfoXml.Unlock();

	m_recordInfo.InitFromXml(strRecordInfoXml);
	m_pRecord = m_recordInfo.CreateRecord();

	// make sure we are at the first record in the file
	assert(m_pFile->Tell() == int(sizeof(m_header) + m_header.userHdr.nMetaInfoLen * sizeof(U16unit)));
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
		if (m_vRecordBlockIndexPos.size() == 0)
		{
			m_pFile->LSeek(m_header.userHdr.nRecordBlockIndexPos);
			unsigned nNewArraySize = 0;
			m_pFile->Read(&nNewArraySize, sizeof(nNewArraySize));
			m_vRecordBlockIndexPos.resize(nNewArraySize);

			m_pFile->Read(&*m_vRecordBlockIndexPos.begin(), nNewArraySize * sizeof(int64_t));
		}
		int64_t nNewPos = m_vRecordBlockIndexPos[unsigned(nRecord / RecordsPerBlock)];
		m_pFile->LSeek(nNewPos);
		if (m_pCompressInput.get())
			m_pCompressInput->Reset();
	}
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
		}

		for (unsigned x = 0; x < numSkipRecs; x++)
			ReadRecord();
	}
}

}}  // namespace Alteryx::OpenYXDB
