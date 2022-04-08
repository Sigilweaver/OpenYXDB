// Copyright 2016, Alteryx, Inc. All rights reserved
//
// This file is distributed to Alteryx customers as part of the
// Software Development Kit.
//
#pragma once

#if !defined(UNICODE)
	#error UNICODE must be defined
#endif

#define _SCL_SECURE_NO_DEPRECATE
#define _U(x) U16(x)

#ifndef SRCLIB_REPLACEMENT
	#define SRCLIB_REPLACEMENT
#endif

#ifdef _WIN64
	#include <Windows.h>
#endif

#if !defined(NOMINMAX)
	#undef min
	#undef max
#endif

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <locale>
#include <string>

#include "Base/StringToDouble.h"

#include "SRC_stringHelper.h"
#include "UnicodeCompareNoCase.h"

#define sizeofArray(a) (sizeof(a) / sizeof(a[0]))

namespace SRC {
#define CP_LATIN1 28591
#define EngCodePage CP_LATIN1

inline const size_t& src_min_size_t(const size_t& a, const size_t& b)
{
	return (b < a) ? b : a;
}  // src_min

template <typename T_Char>
int TStrLen(const T_Char* p)
{
	const T_Char* pOrig = p;
	while (*p)
		++p;
	return int(p - pOrig);
}

template <class T_Char, class T_char_traits = std::char_traits<T_Char>>
class Tstr : public std::basic_string<T_Char, T_char_traits>
{
	inline static int DoCompareNoCase(const char* pA, const char* pB)
	{
#ifdef _WIN64
		return _stricmp(pA, pB);
#else
		return strcasecmp(pA, pB);
#endif
	}

	inline static int DoCompareNoCase(const U16unit* pA, const U16unit* pB)
	{
		return CompareNoCaseUnicode::CompareNoCaseUTF16(pA, pB);
	}

public:
	typedef T_Char TChar;

	inline Tstr()
	{
	}

	inline Tstr(const TChar* p)
		: std::basic_string<TChar, T_char_traits>(p)
	{
	}

	Tstr(const TChar* p, int len)
		: std::basic_string<TChar, T_char_traits>(p, len)
	{
	}

	inline unsigned Length() const
	{
		return unsigned(std::basic_string<TChar, T_char_traits>::length());
	}

	inline unsigned c_strLength() const
	{
		return Length();
	}

	inline bool IsEmpty() const
	{
		return std::basic_string<TChar, T_char_traits>::empty();
	}

	Tstr& Assign(const TChar* p, int len)
	{
		std::basic_string<TChar, T_char_traits>::assign(p, size_t(len));
		return *this;
	}

	inline TChar* Lock(int nLen = -1)
	{
		if (nLen < 0)
			nLen = unsigned(Length());
		Tstr<TChar, T_char_traits> dest;
		dest.resize(nLen);
		memcpy(
			const_cast<TChar*>(dest.c_str()),
			std::basic_string<TChar, T_char_traits>::c_str(),
			std::min(unsigned(nLen), Length() + 1) * sizeof(TChar));
		*this = dest;
		return const_cast<TChar*>(std::basic_string<TChar, T_char_traits>::c_str());
	}

	inline void Unlock()
	{
		const TChar* pBegin = std::basic_string<TChar, T_char_traits>::c_str();
		const TChar* pEnd = pBegin;
		while (*pEnd)
			pEnd++;

		this->resize(pEnd - pBegin);
	}

	inline void Unlock(unsigned nLen)
	{
		std::basic_string<TChar, T_char_traits>::resize(nLen);
	}

	inline operator const TChar*() const
	{
		return std::basic_string<TChar, T_char_traits>::c_str();
	}

	inline Tstr<TChar, T_char_traits> ReplaceString(
		const Tstr<TChar, T_char_traits>& target,
		const Tstr<TChar, T_char_traits>& replacement) const
	{
		Tstr<TChar, T_char_traits> dest = *this;
		typename std::basic_string<TChar, T_char_traits>::size_type pos = 0;
		while ((pos = dest.find(target, pos)) != std::basic_string<TChar, T_char_traits>::npos)
		{
			dest.replace(pos, target.length(), replacement);
			pos += target.length();
		}

		return dest;
	}

	inline Tstr<TChar, T_char_traits>& Assign(int n)
	{
		TChar buffer[256];
		StringHelper::sh_itostr(n, buffer, sizeofArray(buffer));
		*this = buffer;
		return *this;
	}

	explicit Tstr(int n)
	{
		this->Assign(n);
	}

	inline Tstr<TChar, T_char_traits>& Assign(int64_t n)
	{
		TChar buffer[256];
		StringHelper::sh_i64tostr(n, buffer, sizeofArray(buffer));
		*this = buffer;
		return *this;
	}

	explicit Tstr(int64_t n)
	{
		this->Assign(n);
	}

	Tstr<TChar, T_char_traits>& Assign(const double d)
	{
		TChar buffer[DoubleToString::ConvertDoubleBufferSize];
		DoubleToString::Convert(buffer, d);
		*this = buffer;
		return *this;
	}

	explicit Tstr(double n)
	{
		this->Assign(n);
	}

	inline Tstr<TChar, T_char_traits>& Assign(double d, int iDecimals)
	{
#ifndef _CVTBUFSIZE
	#define _CVTBUFSIZE 512
#endif
		TChar buffer[_CVTBUFSIZE];
		StringHelper::sh_dtostr(buffer, _CVTBUFSIZE, iDecimals, d);
		*this = buffer;
		return *this;
	}

	inline double ConvertToDouble() const
	{
		return ::SRC::ConvertToDouble(std::basic_string<TChar, T_char_traits>::c_str());
	}

	inline int ConvertToInt() const
	{
		return StringHelper::sh_strtoi(std::basic_string<TChar, T_char_traits>::c_str());
	}

	inline int64_t ConvertToInt64() const
	{
		return StringHelper::sh_strtoi64(std::basic_string<TChar, T_char_traits>::c_str());
	}

	inline void Truncate(unsigned n)
	{
		if (n < Length())
			std::basic_string<TChar, T_char_traits>::resize(n);
	}

	inline void TruncatePoints(unsigned n)
	{
		Truncate(n);
	}

	inline unsigned LengthPoints() const
	{
		return Length();
	}

	inline void Append(const TChar* p, size_t nLen)
	{
		this->append(p, nLen);
	}

	inline void Append(const TChar* p)
	{
		const TChar* pEnd = p;
		while (*pEnd)
			pEnd++;

		this->append(p, pEnd - p);
	}

	inline Tstr<TChar, T_char_traits>& TrimLeft()
	{
		while (!IsEmpty() && CType::IsSpace(*(std::basic_string<TChar, T_char_traits>::end() - 1)))
			std::basic_string<TChar, T_char_traits>::resize(std::basic_string<TChar, T_char_traits>::size() - 1);
		return *this;
	}

	inline Tstr<TChar, T_char_traits>& TrimRight()
	{
		size_t i = 0;
		for (; i < std::basic_string<TChar, T_char_traits>::size(); i++)
		{
			if (!CType::IsSpace((*this)[i]))
			{
				break;
			}
		}
		*this = Tstr<TChar, T_char_traits>(std::basic_string<TChar, T_char_traits>::c_str() + i);
		return *this;
	}

	inline Tstr<TChar, T_char_traits>& Trim()
	{
		TrimRight();
		return TrimLeft();
	}

	friend Tstr<TChar, T_char_traits> operator+(const Tstr<TChar, T_char_traits>& a, const TChar* b)
	{
		Tstr<TChar, T_char_traits> s(a);
		s += b;
		return s;
	}

	friend Tstr<TChar, T_char_traits> operator+(
		const Tstr<TChar, T_char_traits>& a,
		const Tstr<TChar, T_char_traits>& b)
	{
		Tstr<TChar, T_char_traits> s(a);
		s += b;
		return s;
	}

	friend Tstr<TChar, T_char_traits> operator+(const TChar* a, const Tstr<TChar, T_char_traits>& b)
	{
		Tstr<TChar, T_char_traits> s(a);
		s += b;
		return s;
	}

	inline static int strlen(const TChar* p)
	{
		const TChar* pOrig = p;
		while (*p)
			++p;
		return int(p - pOrig);
	}

	inline static const TChar* strchr(const TChar* p, TChar c)
	{
		for (; *p; ++p)
		{
			if (*p == c)
				return p;
		}
		return nullptr;
	}

	inline static const TChar* strstr(const TChar* str1, const TChar* str2)
	{
		const TChar* cp = str1;
		const TChar *s1, *s2;

		if (!*str2)
			return str1;

		while (*cp)
		{
			s1 = cp;
			s2 = str2;

			while (*s1 && *s2 && !(*s1 - *s2))
				s1++, s2++;

			if (!*s2)
				return (cp);

			cp++;
		}

		return nullptr;
	}

	/** case sensitive compare, returning pA[n]-pB[n] when they're
		* different; return zero (equal) at a null or after comparing nLength */
	inline static int strncmp(const TChar* pA, const TChar* pB, size_t nLength)
	{
		if (nLength == 0)
			return 0;

		const TChar* pEnd = pA + nLength - 1;

		while (*pA && (pA < pEnd) && *pA == *pB)
			pA++, pB++;

		return *(pA) - *(pB);
	}

	inline static int Tstrtol(const char* nptr, char** endptr, int base)
	{
		return StringHelper::sh_strtoi(nptr, endptr, base);
	}

	inline static int Tstrtol(const U16unit* nptr, U16unit** endptr, int base)
	{
		return StringHelper::sh_strtoi(nptr, endptr, base);
	}

	inline static double Tstrtod(const char* nptr, const char** endptr)
	{
		double ret{ 0 };
		auto len = SRC::ConvertToDouble<char>(nptr, ret);
		if (endptr)
			(*endptr) = nptr + len;
		return ret;
	}

	inline static double Tstrtod(const U16unit* nptr, const U16unit** endptr)
	{
		double ret{ 0 };
		auto len = SRC::ConvertToDouble<U16unit>(nptr, ret);
		if (endptr)
			(*endptr) = nptr + len;
		return ret;
	}

	inline static int64_t Tstrtoll(const char* nptr, char** endptr, int base)
	{
		return StringHelper::sh_strtoi64(nptr, endptr, base);
	}

	inline static int64_t Tstrtoll(const U16unit* nptr, U16unit** endptr, int base)
	{
		return StringHelper::sh_strtoi64(nptr, endptr, base);
	}

	inline static int strtoi(const char* p)
	{
		return atoi(p);
	}

	inline static int strtoi(const U16unit* p)
	{
		return StringHelper::sh_strtoi(p);
	}

	/** just like strcasecmp or stricmp, for whichever kind of string you have */
	inline static int CompareNoCase(const TChar* pA, const TChar* pB)
	{
		return CompareNoCaseUnicode::CompareNoCaseUTF16(pA, pB);
	}
};

typedef Tstr<char, std::char_traits<char>> AString;
typedef Tstr<U16unit, std::char_traits<U16unit>> WString;
typedef Tstr<U16unit, std::char_traits<U16unit>> String;

template <class TChar>
WString MSG_NoXL(
	const TChar* msgKey,
	const U16unit* a1 = nullptr,
	const U16unit* a2 = nullptr,
	const U16unit* a3 = nullptr,
	const U16unit* a4 = nullptr);

class char_traits_no_case : public std::char_traits<U16unit>
{
public:
	static int compare(const char_type* s1, const char_type* s2, size_t n)
	{
		assert(static_cast<int>(n) < std::numeric_limits<int>::max());
		return CompareNoCaseUnicode::CompareNoCaseUTF16(s1, s2, static_cast<int>(n));
	}
};

class WStringNoCase : public Tstr<U16unit, char_traits_no_case>
{
public:
	inline WStringNoCase()
	{
	}

	inline WStringNoCase(const U16unit* p)
		: Tstr<U16unit, char_traits_no_case>(p)
	{
	}

	inline WStringNoCase(const WString& str)
		: Tstr<U16unit, char_traits_no_case>(reinterpret_cast<const WStringNoCase&>(str))
	{
	}

	inline operator WString&()
	{
		return reinterpret_cast<WString&>(*this);
	}

	friend WStringNoCase operator+(const WStringNoCase& a, const U16unit* b)
	{
		WStringNoCase s(a);
		s += b;
		return s;
	}

	friend WStringNoCase operator+(const WStringNoCase& a, const WStringNoCase& b)
	{
		WStringNoCase s(a);
		s += b;
		return s;
	}

	friend WStringNoCase operator+(const U16unit* a, const WStringNoCase& b)
	{
		WStringNoCase s(a);
		s += b;
		return s;
	}
};

typedef WStringNoCase StringNoCase;

inline void ConvertString(AString& dest, const U16unit* p, int len = -1)
{
	if (p && *p)
	{
		if (len < 0)
			len = TStrLen(p);
		dest.resize(len, ' ');
		// we can get away with this because dest is local and can't be shared
		char* pRet = const_cast<char*>(dest.c_str());

#ifdef __GNUG__
		for (int x = 0; x < len; ++x)
		{
			if (p[x] >= 256)
			{
				pRet[x] = '?';
			}
			else
				pRet[x] = char(p[x]);
		}
#else
		::WideCharToMultiByte(28591, 0, p, len, pRet, len, "?", nullptr);
#endif

		pRet[len] = 0;
	}
}

inline void ConvertString(AString& dest, const char* p, int len = -1)
{
	(void)len;
	dest = p;
}

inline AString ConvertToAString(const U16unit* p)
{
	AString dest;
	ConvertString(dest, p);
	return dest;
}

inline void ConvertString(WString& dest, const U16unit* p, int len = -1)
{
	(void)len;
	dest = p;
}

inline void ConvertString(WString& dest, const char* p, int len = -1, int nCodePage = CP_LATIN1)
{
	dest.clear();
	if (p && *p)
	{
		assert(nCodePage == CP_LATIN1);  // option is available for uses outside the SDK
		if (len < 0)
			len = unsigned(strlen(p));
		// default 8859-1 conversion
		dest.reserve(len);

		for (int x = 0; x < len; x++)
			dest.push_back((unsigned char)p[x]);
	}
}

inline WString ConvertToWString(const char* p)
{
	WString dest;
	ConvertString(dest, p);
	return dest;
}

inline WString ConvertToWString(const WString& str)
{
	return str;
}

inline String ConvertToString(const char* p)
{
	String dest;
	ConvertString(dest, p);
	return dest;
}

inline String ConvertToString(const U16unit* p)
{
	String dest(p);
	return dest;
}

template <class TChar>
class TCompare_StrCompactWhitespace
{
public:
	static int Compare(const TChar* pA, const TChar* pB)
	{
		int nRet = 0;
		bool bInQuotes = false;
		while (*pA && *pB)
		{
			TChar a = *pA;
			TChar b = *pB;

			if (!bInQuotes)
			{
				while (a && CType::IsSpace(a))
					a = *++pA;
				while (b && CType::IsSpace(b))
					b = *++pB;
			}

			if (a != b)
			{
				nRet = a - b;
				break;
			}

			if (a)
			{
				++pA;
				++pB;
				if (a == '"')
					bInQuotes = !bInQuotes;
			}
		}
		return nRet;
	}

	bool operator()(const TChar* pA, const TChar* pB) const
	{
		return Compare(pA, pB) < 0;
	}
};

typedef TCompare_StrCompactWhitespace<U16unit> Compare_StrCompactWhitespace;

class Error
{
	String m_strError;

public:
	inline Error(const String& str)
		: m_strError(str) /* XMSG() */
	{
	}
	inline Error(const AString& str)
		: m_strError(MSG_NoXL(str.c_str()))
	{
	}
	inline Error(const U16unit* str)
		: m_strError(str)
	{
	}
	inline Error(const WStringNoCase& str)
		: m_strError(str)
	{
	}

	const String& GetErrorDescription() const
	{
		return m_strError;
	}

	const String& GetErrorDescriptionW() const
	{
		return m_strError;
	}
};
///////////////////////////////////////////////////////////////////////////////
//
//	class ErrorUserCanceled
//
///////////////////////////////////////////////////////////////////////////////
class ErrorUserCanceled : public Error
{
public:
	inline ErrorUserCanceled()
		: Error(MSG_NoXL("User Canceled"))
	{
	}

	//		inline ErrorUserCanceled(const ErrorUserCanceled &e) : Error(e) { }
};  // ErrorUserCanceled

#define HAS_BlobDataRead
class BlobDataRead
{
	mutable const void* m_pBlobNext;
	const void* m_pBlobEnd;

public:
	inline BlobDataRead(const void* pBlob, size_t nSize)
		: m_pBlobNext(pBlob)
		, m_pBlobEnd(static_cast<const char*>(pBlob) + nSize)
	{
	}

	inline bool IsEof()
	{
		return m_pBlobNext >= m_pBlobEnd;
	}

	inline unsigned Read(void* pDest, unsigned nSize) const
	{
		if ((static_cast<const char*>(m_pBlobNext) + nSize) > m_pBlobEnd)
			throw Error(MSG_NoXL("Internal Error: Attempt to read past the end of a blob."));

		memcpy(pDest, m_pBlobNext, nSize);
		m_pBlobNext = static_cast<const char*>(m_pBlobNext) + nSize;

		return nSize;
	}

	inline const void* Get(unsigned nSize) const
	{
		if ((static_cast<const char*>(m_pBlobNext) + nSize) > m_pBlobEnd)
			throw Error(MSG_NoXL("Internal Error: Attempt to read past the end of a blob."));

		const void* pRet = m_pBlobNext;
		m_pBlobNext = static_cast<const char*>(m_pBlobNext) + nSize;
		return pRet;
	}
};

namespace SHPBlob {
template <class TFile>
bool ValidateShpBlob(TFile& file, bool bFileMode = false)
{
	(void)file;
	(void)bFileMode;
	return false;
}
}  // namespace SHPBlob

template <class TDataType>
class SmartPointerRefObj
{
	TDataType* pData;

	inline void DereferenceObj()
	{
		if (pData)
		{
#ifdef __GNUG__
			//__sync_fetch_and_add(&pData->m_SmartPointerRefObj_refCount, -1);
			//@todo: Check this!
			(pData->m_SmartPointerRefObj_refCount)--;
			if ((&pData->m_SmartPointerRefObj_refCount) == 0)
#else
			if (InterlockedDecrement(&pData->m_SmartPointerRefObj_refCount) == 0)
#endif
			{
				delete pData;
				pData = nullptr;
			}
		}
	}
	inline void ReferenceObj()
	{
		if (pData)
#ifdef __GNUG__
			//__sync_fetch_and_add(&pData->m_SmartPointerRefObj_refCount, 1);
			//@todo: Check this!
			(pData->m_SmartPointerRefObj_refCount)++;
#else
			InterlockedIncrement(&pData->m_SmartPointerRefObj_refCount);
#endif
	}

public:
	///////////////////////////////////////////////////////////////////////////////
	// Function name	: SmartPointerRefObj
	// Description:   This version of the consructor will own the pointer that is passed in
	//					DO NOT delete the pointer yourself.  Make sure the pointer is to dynamicly allocated data
	// Return "inline":
	// Arguments:
	//    const TDataType * o:
	///////////////////////////////////////////////////////////////////////////////
	inline SmartPointerRefObj(TDataType* o = nullptr)
		: pData(o)
	{
		ReferenceObj();
	}

	inline SmartPointerRefObj(const SmartPointerRefObj<TDataType>& o)
	{
		if (!o.pData)
			pData = nullptr;
		else
		{
			pData = o.pData;
			ReferenceObj();
		}
	}

	// KLUDGE: this says it takes a const, but the only thing that is const is the pointer.
	// the thing the pointer points to can change
	inline SmartPointerRefObj<TDataType>& operator=(const SmartPointerRefObj<TDataType>& o)
	{
		DereferenceObj();
		if (!o.pData)
			pData = nullptr;
		else
		{
			pData = o.pData;
			ReferenceObj();
		}
		return *this;
	}

	inline SmartPointerRefObj<TDataType>& operator=(TDataType* pO)
	{
		DereferenceObj();

		pData = pO;
		ReferenceObj();

		return *this;
	}

	//////////////////////////////////////////////////////////////////
	// Note: the three comparison operators defined here compare the pointers,
	// not the data pointed to.  Thus pointers to two copies of exactly the same
	// data will not be equal.
	//////////////////////////////////////////////////////////////////
	inline bool operator==(const SmartPointerRefObj<TDataType>& o) const
	{
		return pData == o.pData;
	}

	inline bool operator!=(const SmartPointerRefObj<TDataType>& o) const
	{
		return pData != o.pData;
	}

	inline bool operator<(const SmartPointerRefObj<TDataType>& o) const  // RB 1/6/00
	{
		return pData < o.pData;
	}

	inline ~SmartPointerRefObj()
	{
		DereferenceObj();
	}

	inline const TDataType* operator->() const
	{
		assert(pData);
		return pData;
	}

	inline TDataType* operator->()
	{
		assert(pData);
		return pData;
	}

	inline const TDataType& operator*() const
	{
		assert(pData);
		return *(pData);
	}

	inline TDataType& operator*()
	{
		assert(pData);
		return *(pData);
	}

	inline void Delete()
	{
		DereferenceObj();
		pData = nullptr;
	}

	inline TDataType* Get()
	{
		return pData ? pData : nullptr;
	}

	inline const TDataType* Get() const
	{
		return pData ? pData : nullptr;
	}
	template <class TDestDataType>
	SmartPointerRefObj<TDestDataType> StaticCast()
	{
		// guard against multiple inheritance...
		// it will also force the compiler to check if this cast is valid
		if (static_cast<TDestDataType*>(pData) != pData)
			throw Error(MSG_NoXL("Internal Error: SmartPointer Static cast is not valid for multiple inheritance."));

		return SmartPointerRefObj<TDestDataType>(reinterpret_cast<SmartPointerRefObj<TDestDataType>&>(*this));
	}

};  // SmartPointerRefObj
///////////////////////////////////////////////////////////////////////////////
// class SmartPointerRefObj
//
// This is similar to the RefCountObj, but without the locking and unlocking,
// It also will never create a new obj.
///////////////////////////////////////////////////////////////////////////////
struct SmartPointerRefObj_Base
{
	template <class T>
	friend class SmartPointerRefObj;
	long m_SmartPointerRefObj_refCount;

protected:
	inline SmartPointerRefObj_Base()
		: m_SmartPointerRefObj_refCount(0)
	{
	}
	inline ~SmartPointerRefObj_Base()
	{
	}
	inline SmartPointerRefObj_Base(const SmartPointerRefObj_Base&)
		: m_SmartPointerRefObj_refCount(0)
	{
	}
	SmartPointerRefObj_Base& operator=(const SmartPointerRefObj_Base&)
	{
		return *this;
	}
};

#define _GLOT_H_

/** ModI18n changed this line, but thinks it's fine */
#define GlotNote

/** ModI18n changed this line, but not sure it's right */
#define GlotWarn

template <class TChar>
WString MSG_NoXL(const TChar* msgKey, const U16unit* a1, const U16unit* a2, const U16unit* a3, const U16unit* a4)
{
	const U16unit* args[] = { a1, a2, a3, a4 };
	WString msg;
	U16unit ch;
	while (0 != (ch = (*msgKey++)))
	{
		if (ch != '@')
		{
			msg += ch;
		}
		else if ('1' <= *msgKey && *msgKey <= '9')
		{
			int argNum = (*msgKey++) - '1';  // gives 0 to nArgs-1
			if (argNum >= 4 || !args[argNum])
			{
				((msg += U16("<Missing Argument Text ")) += *(msgKey - 1)) += '>';
			}
			else
			{
				msg += args[argNum];
			}
		}
		else
		{
			msg += '@';
			if (*msgKey == '@')
				++msgKey;  // skip a second one of them.
		}
	}
	return msg;
}
template <class TChar>
WString XMSG(
	const TChar* msgKey,
	const U16unit* a1 = nullptr,
	const U16unit* a2 = nullptr,
	const U16unit* a3 = nullptr,
	const U16unit* a4 = nullptr)
{
	return MSG_NoXL(msgKey, a1, a2, a3, a4);
}
template <class TChar>
WString XMSG(
	const TChar* msgKey,
	const char* a1,
	const char* a2 = nullptr,
	const char* a3 = nullptr,
	const char* a4 = nullptr)
{
	WString args[4];
	args[0] = ConvertToWString(a1);
	if (a2)
		args[1] = ConvertToWString(a2);
	if (a3)
		args[2] = ConvertToWString(a3);
	if (a4)
		args[3] = ConvertToWString(a4);
	return MSG_NoXL(msgKey, args[0], args[1], args[2], args[3]);
}
}  // namespace SRC
