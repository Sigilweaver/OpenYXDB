// Copyright(C) 2005 - 2019 Alteryx, Inc. All rights reserved.
// This file is distributed to Alteryx customers as part of the
// Software Development Kit.

#pragma once

#include <limits>
#ifndef SRCLIB_REPLACEMENT
	#include "Base/SRC_string.h"
#endif

namespace SRC {

// Wide strings are limited to half of this since it is in bytes, not characters
const unsigned MaxFixedLengthStringSize = 16384;

// MAxFieldLength is also the Max Record length.  In reality the max field length would be a at least 12 bytes less, because you have:
//		the field offset value
//		the total vardata length
//		the field vardata length
const unsigned MaxFieldLength64 = 0x7fffffffu;  // 2GB
const unsigned MaxFieldLength32 = 0x0fffffffu;  // 256MB

#if defined(_WIN64) || defined(E2) || defined(__linux__) || defined(__APPLE__)
const unsigned MaxFieldLength = MaxFieldLength64;
#else
const unsigned MaxFieldLength = MaxFieldLength32;
#endif

// NOTE: used to be 350 but the BCD can only contain 256 digits, plus sign and decimal
const unsigned MaxFixedDecimalPrecision = 258;  //350;

class GenericEngineBase
{
protected:
	mutable unsigned m_nFieldConversionErrorLimit;

public:
	enum MessageType
	{
		// these ID's are set to these values just to make it easy on Alteryx and no harder on other apps.
		MT_Info = 1,
		MT_Warning = 2,
		MT_FieldConversionError = 5,
		MT_FieldConversionLimitReached = 6,
		MT_Error = 3,
		MT_ConnectInfoXml = 17,
		MT_SafeModeError = 21,
	};

	inline GenericEngineBase(unsigned nFieldConversionErrorLimit)
		: m_nFieldConversionErrorLimit(nFieldConversionErrorLimit)
	{
		if (m_nFieldConversionErrorLimit == 0)
			m_nFieldConversionErrorLimit = std::numeric_limits<unsigned>::max();
	}

	virtual ~GenericEngineBase()
	{
	}

	inline unsigned GetFieldConversionErrorLimit() const
	{
		return m_nFieldConversionErrorLimit;
	}

	virtual long OutputMessage(MessageType mt, const U16unit* pMessage) const = 0;
	// TODO E2 - figure out what we are doing with generic engine and test unicode
	virtual long OutputMessage(MessageType mt, const char* pMessage) const
	{
		return OutputMessage(mt, ConvertToWString(pMessage));
	}

	typedef void (*ThreadProc)(void* pData);
	virtual void QueueThread(ThreadProc pProc, void* pData) const = 0;

	virtual const U16unit* GetInitVar2(int /*nToolId*/, const U16unit* /*pVar*/) const
	{
		return U16("");
	}

	// returns true to cancel processing
	// this can be called if there will be long periods of processing with no progress to the user
	virtual bool Ping() const = 0;
};

class NullEngine
{
public:
	typedef void (*ThreadProc)(void* pData);
	virtual void QueueThread(ThreadProc /*pProc*/, void* /*pData*/) const
	{
	}
	virtual bool Ping() const
	{
		return true;
	}
};
}  // namespace SRC