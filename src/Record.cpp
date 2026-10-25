#include "stdafx.h"

#include "RecordLib/Record.h"

#include "RecordLib/FieldBase.h"
#include "RecordLib/RecordData.h"

namespace SRC {
void Record::Allocate(unsigned nNewMinimumVarDataSize)
{
	unsigned nMinSize = (m_nFixedRecordSize + 4 + nNewMinimumVarDataSize);

	if (nMinSize > MaxFieldLength)
	{
		if (nMinSize < MaxFieldLength64)
			throw Error(ErrorKind::Format, XMSG(
				"This file has large records that are not supported by the 32 bit version.  Use the 64 bit version instead."));
		throw Error(
			ErrorKind::Format, XMSG("Record too big:  Records are limited to @1 bytes.", String(static_cast<int64_t>(MaxFieldLength))));
	}

	if (m_nCurrentBufferSize < nMinSize)
	{
		if (m_bContainsVarData)
		{
			m_nCurrentBufferSize = nMinSize;
			m_nCurrentBufferSize *= 2;
			if (m_nCurrentBufferSize > MaxFieldLength)
				m_nCurrentBufferSize = MaxFieldLength;
		}
		else
			m_nCurrentBufferSize = m_nFixedRecordSize;

		// The following assert is a soft limit, not a hard one.
		// It is here to help find random uninitialized memory errors.
		m_pRecord = realloc(m_pRecord, m_nCurrentBufferSize);
		if (m_pRecord == 0)
			throw Error(
				XMSG("Unable to allocate @1 bytes of memory.", String(static_cast<int64_t>(m_nCurrentBufferSize))));
	}
}

void Record::Init(int nFixedRecordSize, bool bContainsVarData)
{
	m_nFixedRecordSize = nFixedRecordSize;
	m_bContainsVarData = bContainsVarData;
	Allocate(0);
	Reset();
}

void Record::Reset(int nVarDataSize)
{
	m_nCurrentVarDataSize = nVarDataSize;
	m_bVarDataLenUnset = true;
	assert(m_pRecord != nullptr);
}

Record::~Record()
{
	if (m_pRecord)
		free(m_pRecord);
}

// returns the offset within the var data to the start
int Record::AddVarData(const void* pVarData, unsigned nLen)
{
	if ((m_nFixedRecordSize + 4 + m_nCurrentVarDataSize + nLen) > MaxFieldLength)
	{
		throw SRC::Error(ErrorKind::Conversion, XMSG(
			"Record too big:  Records are limited to @1 bytes", SRC::String(static_cast<int64_t>(MaxFieldLength))));
	}

	m_bVarDataLenUnset = true;

	// we return the actual offset in the vardata , including the vardata len.  That way, 0 can't happen
	int nRet = m_nCurrentVarDataSize + 4;

	// if the vardata is shorter that 127 characters, only 1 byte will be used
	// for the length, setting the low bit to signal this condition
	// with the length be left shifted by 1 in either case
	// this would have to change for big endian processor architectures
	//int nLenLength;
	unsigned nStoreLen = nLen << 1;
	if (nLen > 0x7f)
	{
		Allocate(m_nCurrentVarDataSize + nLen + sizeof(unsigned));
		*reinterpret_cast<unsigned*>(
			static_cast<char*>(m_pRecord) + m_nFixedRecordSize + sizeof(m_nCurrentVarDataSize)
			+ m_nCurrentVarDataSize) = nStoreLen;
		m_nCurrentVarDataSize += sizeof(unsigned);
	}
	else
	{
		nStoreLen |= 1;
		Allocate(m_nCurrentVarDataSize + nLen + sizeof(unsigned char));
		*reinterpret_cast<unsigned char*>(
			static_cast<char*>(m_pRecord) + m_nFixedRecordSize + sizeof(m_nCurrentVarDataSize)
			+ m_nCurrentVarDataSize) = (unsigned char)nStoreLen;
		m_nCurrentVarDataSize += sizeof(unsigned char);
	}

	memcpy(
		static_cast<char*>(m_pRecord) + m_nFixedRecordSize + sizeof(m_nCurrentVarDataSize) + m_nCurrentVarDataSize,
		pVarData,
		nLen);
	m_nCurrentVarDataSize += nLen;
	return nRet;
}

void Record::SetLength() const
{
	m_bVarDataLenUnset = false;
	memcpy(static_cast<char*>(m_pRecord) + m_nFixedRecordSize, &m_nCurrentVarDataSize, sizeof(m_nCurrentVarDataSize));
}

RecordData* Record::GetRecord()
{
	if (m_bVarDataLenUnset && m_bContainsVarData)
		SetLength();

	return static_cast<RecordData*>(m_pRecord);
}

const RecordData* Record::GetRecord() const
{
	if (m_bVarDataLenUnset && m_bContainsVarData)
		SetLength();

	return static_cast<const RecordData*>(m_pRecord);
}

int Record::GetVarDataSize()
{
	return m_nCurrentVarDataSize;
}

}  // namespace SRC
