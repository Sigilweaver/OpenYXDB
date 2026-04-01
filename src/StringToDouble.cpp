#include "stdafx.h"

#include "Base/StringToDouble.h"

#if defined(__linux__) || defined(__APPLE__)
	#include <climits>
	#include <cmath>
	#include <cstring>
	#include <float.h>
#endif

#include <cassert>
#include <cstdint>
#include <memory>

#include "Base/SRC_doubleToStringHelper.h"

#ifndef SRCLIB_REPLACEMENT
	#include <boost/predef/other/endian.h>
#endif

#ifdef _WIN64
	#include <intrin.h>
#endif

namespace SRC {
namespace {
#ifndef SRCLIB_REPLACEMENT
static_assert(BOOST_ENDIAN_LITTLE_BYTE, "The layout of this struct is only correct on Little Endian.");
#endif

struct IEEEDbl
{
	uint32_t mantl : 32;
	uint32_t manth : 20;
	uint32_t expo : 11;
	uint32_t sign : 1;
};
union DoubleBits
{
	IEEEDbl bits;
	double dbl;
};

// This is a useful thing on its own, but we don't seem to need it anywhere else.
template <int NWords>
class BigInt
{
public:  // constructors
	// initialize to zero
	BigInt();

public:
	/** Multiply by the fraction N/D. Rounds the result. It's up to you to not cause overflow. */
	void MultByFraction(int N, int D);

	/** Set a word */
	void Set(int pos, uint32_t val)
	{
		m_Words[pos] = val;
	}

	/** Get top words, rounded */
	uint64_t GetMSW() const;

	/** Get a word */
	uint32_t GetW(int pos) const
	{
		return m_Words[pos];
	}

	/** Shift left one bit */
	void ShiftLeft();

	/** Shift Right one bit */
	void ShiftRight();

private:  // data
	uint32_t m_Words[NWords];
};

template <int NWords>
BigInt<NWords>::BigInt()
{
	for (int j = NWords; (--j) >= 0;)
		m_Words[j] = 0;
}

template <int NWords>
uint64_t BigInt<NWords>::GetMSW() const
{
	uint64_t res = m_Words[NWords - 1];
	res = (res << 32) + m_Words[NWords - 2];
	// If the third word is over half, or if it is exactly half and the second word is odd, round up
	if (NWords > 2 && ((m_Words[NWords - 3] + (m_Words[NWords - 2] & 1)) > 0x80000000u))
		++res;
	return res;
}

template <int NWords>
void BigInt<NWords>::ShiftLeft()
{
	for (int j = NWords; (--j) > 0;)
		m_Words[j] = (m_Words[j] << 1) | (m_Words[j - 1] >> 31);
	m_Words[0] = m_Words[0] << 1;
}

template <int NWords>
void BigInt<NWords>::ShiftRight()
{
	for (int j = 0; j < (NWords - 1); ++j)
		m_Words[j] = (m_Words[j] >> 1) | (m_Words[j + 1] << 31);
	m_Words[NWords - 1] = m_Words[NWords - 1] >> 1;
}

template <int NWords>
void BigInt<NWords>::MultByFraction(int N, int D)
{
	uint32_t scratch[NWords + 1];
	scratch[0] = D / 2;  // initialize with half of D to "round" the result
	// fill scratch with N*m_Words + D/2
	for (int j = 0; j < NWords; ++j)
	{
		uint64_t acc = static_cast<uint64_t>(m_Words[j]) * N + static_cast<uint64_t>(scratch[j]);
		scratch[j] = static_cast<uint32_t>(acc);
		scratch[j + 1] = acc >> 32;
	}
	// now divide by D, putting the result back into m_Words
	for (int j = NWords; (--j) >= 0;)
	{
		uint64_t acc = scratch[j + 1];
		acc = (acc << 32) + scratch[j];
		m_Words[j] = static_cast<uint32_t>(acc / D);
		assert((acc / D) <= UINT32_MAX);  // if this fails, something overflowed.
		scratch[j] = acc % D;
	}
}

constexpr inline bool IsNumber(int val)
{
	return (('0' <= val && val <= '9') || val == '.');
}

constexpr inline bool IsSpace(int val)
{
	return (val == ' ' || val == '\n' || val == '\t' || val == '\r');
}

// this represents a floating point number with value GetMant * 2^m_Pow2.
// GetMant is to be interpreted as a fraction, 0.5<= GetMant < 1.0
// I only use these to represent the power of 10 table.
struct FloatFrac
{
	uint64_t m_Mant;
	int16_t m_Pow2;
	uint64_t GetMant() const
	{
		return m_Mant;
	}
	void SetMant(uint64_t val)
	{
		m_Mant = val;
	}
};

const int NWords = 3;
const uint32_t TopBit = 0x80000000u;

std::unique_ptr<FloatFrac[]> MakePosTable(int NumPow10)
{
	std::unique_ptr<FloatFrac[]> powTable = std::make_unique<FloatFrac[]>(NumPow10);
	BigInt<NWords> mant;
	int16_t expo = 1;
	mant.Set(NWords - 1, TopBit);
	for (int pow10 = 0; pow10 < NumPow10; ++pow10)
	{
		powTable[pow10].SetMant(mant.GetMSW());
		powTable[pow10].m_Pow2 = expo;
		// scale before multiplying, so that after multiplying I keep the top bit
		// set, I keep a real 96 bits of precision.
		if (mant.GetW(NWords - 1) >= (0x3ffffffffull / 5))
		{
			mant.ShiftRight();
			++expo;
		}
		// This multiplies by 5/4 * 8, that is, 10.
		mant.MultByFraction(5, 4);
		expo += 3;
	}
	return powTable;
}

std::unique_ptr<FloatFrac[]> MakeNegTable(int NumPow10)
{
	std::unique_ptr<FloatFrac[]> powTable = std::make_unique<FloatFrac[]>(NumPow10);
	BigInt<NWords> mant;
	int16_t expo = 1;
	mant.Set(NWords - 1, TopBit);
	for (int pow10 = 0; pow10 < NumPow10; ++pow10)
	{
		// mant as a fraction  0.5 <= mant <1.0 times 2^expo
		// equals 10^-pow10
		powTable[pow10].SetMant(mant.GetMSW());
		powTable[pow10].m_Pow2 = expo;
		// now advance to the next one. val * 4 / 5 / 8
		mant.MultByFraction(4, 5);
		expo -= 3;
		// scale after multiplying, keep the top bit set
		if (!(mant.GetW(NWords - 1) & TopBit))
		{
			mant.ShiftLeft();
			--expo;
		}
	}
	return powTable;
}

// multiply two int64 interpreted as fractions, 0.5 <= N < 1.0.
void MultFrac(uint64_t& mant, uint64_t fac)
{
	const uint64_t lmask = 0xFFFFFFFFull;
	uint64_t sumhh = (mant >> 32) * (fac >> 32);
	uint64_t sumlh = (mant & lmask) * (fac >> 32);
	uint64_t sumhl = (mant >> 32) * (fac & lmask);
	sumhh += (sumlh >> 32) + (sumhl >> 32);
	sumlh &= lmask;
	sumlh += (sumhl & lmask) + 0x7FFFFFFFull;  // round into the top 32 bits
	mant = sumhh + (sumlh >> 32);
}

// count leading zeros.
inline int clz(uint64_t mant)
{
#ifdef E2LINUX
	return __builtin_clzll(mant);
#elif defined(_WIN64)
	unsigned long index;
	_BitScanReverse64(&index, mant);
	return static_cast<int>(63 - index);
	// doesn't work on some chip sets still in use.
	//return static_cast<int>(__lzcnt64(mant));
#else
	int lz = 0;
	if (!(mant & 0xffffffff00000000))
	{
		mant <<= 32;
		lz = 32;
	}
	if (!(mant & 0xffff000000000000))
	{
		mant <<= 16;
		lz += 16;
	}
	if (!(mant & 0xff00000000000000))
	{
		mant <<= 8;
		lz += 8;
	}
	if (!(mant & 0xf000000000000000))
	{
		mant <<= 4;
		lz += 4;
	}
	if (!(mant & 0xC000000000000000))
	{
		mant <<= 2;
		lz += 2;
	}
	if (!(mant & 0x8000000000000000))
	{
		// mant <<= 1;
		lz += 1;
	}
	return lz;
#endif
}

// convert from integer mant * 10^exp10, to fraction mant * 2^exp2
// doesn't end up guaranteeing that the MSB of mant is in the exactly right place, but
// it should be close.
int Normalize(uint64_t& mant, int exp10)
{
	assert(mant != 0);  // avoid infinite loops trying to normalize.
						// The value comes in aa an integer times a power of 10. First normalize the
						// mantissa into a 0.5 <= mant < 1.0 (binary point at the left of the uint64)
	int lz = clz(mant);
	int exp2 = 64 - lz;  // 64 'cause it was originally a fraction times 2^64 to make an int
	mant <<= lz;

	uint64_t fac;
	if (exp10 >= 0)
	{
		const int MaxPow10 = 310;
		static auto PowTableP = MakePosTable(MaxPow10);
		if (exp10 >= MaxPow10)
		{
			return 1000000;  // Infinite
		}
		exp2 += PowTableP[exp10].m_Pow2;
		fac = PowTableP[exp10].GetMant();
	}
	else
	{
		const int MinPow10 = -350;
		static auto PowTableN = MakeNegTable(-MinPow10);
		if (exp10 <= MinPow10)
		{
			mant = 0;  // it's too small even for denormalized.
			return 0;
		}
		else
		{
			exp2 += PowTableN[-exp10].m_Pow2;
			fac = PowTableN[-exp10].GetMant();
		}
	}
	MultFrac(mant, fac);
	assert(mant != 0);
	return exp2;
}

// given a 0.5<= mant < 1.0, * 2^exp2, convert it into a proper binary double.
double MakeDouble(bool isNeg, int exp2, uint64_t mant)
{
	DoubleBits dbits;
	dbits.bits.sign = isNeg ? 1 : 0;
	if (mant == 0)
		return 0;

	// make sure mant < 2^63
	if ((mant & 0x8000000000000000))
	{
		mant >>= 1;
		++exp2;
	}
	// not expecting that this will hit more than a few times, so a loop will be fine.
	while (!(mant & 0x4000000000000000))
	{
		mant <<= 1;
		--exp2;
	}
	// shift right 10 bits, but round to even. This can concievably overflow, which is
	// why I moved it down one bit first.
	mant = (mant + 0x1ff);  //+ ((mant >> 10) & 1)
	mant >>= 10;
	if ((mant & 0x20000000000000))
	{
		// ZOW! overflow when rounding! The smallest normal number can cause this: 2.225073858507201383e-308
		mant >>= 1;
		++exp2;
	}
	assert(0x10000000000000ull <= mant && mant < 0x20000000000000ull);
	// bottom 53 bits of mant are the good ones.
	exp2 += 1021;  // now it is adjusted to how it is stored into a double.
	if (exp2 > 2046)
	{
		// too big, you get an infinity
		dbits.bits.expo = 2047;
		dbits.bits.manth = 0;
		dbits.bits.mantl = 0;
	}
	else if (exp2 <= 0)
	{
		// too small, need a denormalized number
		if (exp2 <= -52)
		{
			// way too small, you get zero (keep the sign)
			dbits.bits.expo = 0;
			dbits.bits.manth = 0;
			dbits.bits.mantl = 0;
		}
		else
		{
			// make a denormalized number. The stored exponent will be zero, but
			// the number has to be shifted by one more than you'd expect.
			exp2 = 1 - exp2;
			uint64_t mask = 1ull << (exp2 - 1);
			mant += ((mant >> exp2) & 1);
			mant += (mask - 1);
			if ((mant & 0x20000000000000))
			{
				// ZOW! overflow when rounding a denormalized number! The largestest denormalized number can cause this: 2.225073858507200889e-308
				mant >>= 1;
				--exp2;  // backwards from normal, because we're going to use this to shift right.
			}
			mant >>= exp2;
			dbits.bits.expo = 0;
			dbits.bits.manth = mant >> 32;
			dbits.bits.mantl = mant;
		}
	}
	else
	{
		// normal number no overflow.
		dbits.bits.expo = exp2;
		dbits.bits.manth = mant >> 32;
		dbits.bits.mantl = mant;
	}
	return dbits.dbl;
}
}  // namespace

#ifdef _WIN32
template <class TNum, int MinNumDigits>
bool DoubleToStringHelper::DoEcvt(char* ret, TNum f)
{
	char* ret_orig = ret;
	char buffer[MinNumDigits + 2];
	int nDecimal, sign;
	_ecvt_s(buffer, sizeofArray(buffer), f, MinNumDigits, &nDecimal, &sign);

	if (nDecimal > MinNumDigits || nDecimal < -MinNumDigits)
		return false;

	bool bDecimal = false;
	if (sign)
		*ret++ = '-';
	if (nDecimal <= 0)
	{
		*ret++ = '0';
		*ret++ = '.';
		bDecimal = true;
		for (; nDecimal < 0; nDecimal++)
			*ret++ = '0';
	}
	else
	{
		memcpy(ret, buffer, nDecimal);
		ret += nDecimal;
		if (nDecimal < MinNumDigits)
		{
			*ret++ = '.';
			bDecimal = true;
		}
	}

	// copy over to the right of the decimal - if any...
	for (char* p = buffer + nDecimal; *p; ++p)
		*ret++ = *p;

	if (nDecimal < MinNumDigits)
	{
		while (*(ret - 1) == '0')
			--ret;
		if (*(ret - 1) == '.')
		{
			--ret;
			bDecimal = false;
		}
	}

	*ret = '\0';

	return (ret - ret_orig) <= (MinNumDigits + (bDecimal ? 1 : 0) + (sign ? 1 : 0));
}
#else
template <class TNum, int MinNumDigits>
void DoubleToStringHelper::DoSnprintf(char* buffer, TNum f)
{
	snprintf(buffer, BufferSize<MinNumDigits>::value, "%.*e", MinNumDigits - 1, f);

	//for iteration
	const char* tmpBuf = buffer;

	//here we will find out where the string of zeros immediately preceding the 'e' begin and end.
	//the point is to take something like A.B0000000e-CD and convert it to A.Be-CD
	size_t startIndex = std::string::npos;
	size_t endIndex = std::string::npos;
	while (*tmpBuf)
	{
		//set the start index only if not already set
		if (*tmpBuf == '0')
			startIndex = (startIndex == std::string::npos) ? (tmpBuf - buffer) : startIndex;
		else
		{
			//set the end index only if start index is set
			if (*tmpBuf == 'e')
			{
				endIndex = (startIndex != std::string::npos) ? (tmpBuf - buffer) : std::string::npos;
				break;
			}
			//unset the start index
			startIndex = std::string::npos;
		}
		++tmpBuf;
	}

	//collapse the zeros part of the string
	if (startIndex != std::string::npos && endIndex != std::string::npos)
	{
		//assert to ensure destination buffer can accommodate the data to be copied
		assert(endIndex < BufferSize<MinNumDigits>::value);
		memcpy(buffer + startIndex, buffer + endIndex, (strlen(buffer) - endIndex) + 1);
	}
}
#endif

template <class TNum, int MinNumDigits>
void DoubleToStringHelper::DoGcvt(char* buffer, TNum f)
{
#ifdef _WIN32
	_gcvt_s(buffer, MinNumDigits <= 7 ? ConvertFloatBufferSize : ConvertDoubleBufferSize, f, MinNumDigits);
#else
	buffer = gcvt(f, MinNumDigits, buffer);
#endif
	auto len = strlen(buffer);
	if (len > 4 && buffer[len - 3] == '0' && (buffer[len - 4] == '-' || buffer[len - 4] == '+'))
	{
		// 3 digit exponent with leading zero.
		assert('0' <= buffer[len - 2] && buffer[len - 2] <= '9' && '0' <= buffer[len - 1] && buffer[len - 1] <= '9');
		buffer[len - 3] = buffer[len - 2];
		buffer[len - 2] = buffer[len - 1];
		buffer[len - 1] = 0;
	}
}

template <class TNum, int MinNumDigits>
void DoubleToStringHelper::Convert(char* buffer, TNum f)
{
#ifdef _WIN32
	#ifdef DEBUG
	// try to cause a buffer overrun in DEBUG to force the buffer to be long enough.
	memset(buffer, 0, MinNumDigits <= 7 ? ConvertFloatBufferSize : ConvertDoubleBufferSize);
	#endif
	// we want exp notation if it takes more than our max digits to display without.
	if (!DoEcvt<TNum, MinNumDigits>(buffer, f))
	{
		DoGcvt<TNum, MinNumDigits>(buffer, f);
	}
#else
	// we want exp notation if it takes more than our max digits to display without.
	if (f != 0
		&& (fabs(f) >= CompileTimePow<TNum, MinNumDigits>::value
			|| fabs(f) < CompileTimeNegPow<TNum, MinNumDigits>::value))
	{
		DoSnprintf<TNum, MinNumDigits>(buffer, f);
	}
	else
	{
		DoGcvt<TNum, MinNumDigits>(buffer, f);
	}
#endif
}

// returns the numbers of characters used in the input string
// see http://krashan.ppa.pl/articles/stringtofloat/
template <typename CharType>
unsigned ConvertToDouble(const CharType* data, double& outVal, char sep)
{
	const int MaxExponent = 300000;
	outVal = 0.0;
	if (!data)
		return 0;
	// skip leading space
	const CharType* cp = data;
	while (IsSpace(*cp))
		++cp;
	// get a sign
	bool isNeg = false;
	if (*cp == '+')
	{
		++cp;
	}
	else if (*cp == '-')
	{
		isNeg = true;
		++cp;
	}
	const CharType* numStart = cp;  // to ensure a digit is read, . +. and -. are not numbers
	// read mantissa
	int mantExp = 0;
	uint64_t mant = 0;
	unsigned ndig;
	for (; (ndig = (*cp - '0')) <= 9; ++cp)
	{
		// read digits before decimal point
		// stop when I'm going to overflow the bits in the uint64 register,
		// not based on the number of digits. Gets me a few more bits sometimes
		if (mant < ((UINT64_MAX / 10) - 1))
		{
			mant = mant * 10 + ndig;
		}
		else if (mantExp < (INT_MAX - 10))
		{
			// can't keep the digits. Keep the exponent, if I can
			++mantExp;
		}
	}
	if (*cp == sep)
	{
		// a fraction part. Here digits we keep decrease the mantissa exponent,
		// digits we drop don't change it
		++cp;
		++numStart;
		for (; (ndig = (*cp - '0')) <= 9; ++cp)
		{
			if (mant < ((UINT64_MAX / 10) - 1))
			{
				mant = mant * 10 + ndig;
				--mantExp;
			}
		}
	}
	if (cp == numStart)
		return 0;  // just + or -, +. or -.
	// Do we have an explicit exponent?
	if (*cp == 'e' || *cp == 'E' || *cp == 'd' || *cp == 'D')
	{
		const CharType* expStart = cp;
		++cp;
		// Sign of exponent
		int expSign = 1;
		int givenExp = 0;
		if (*cp == '+')
		{
			++cp;
		}
		else if (*cp == '-')
		{
			expSign = -1;
			++cp;
		}
		bool first = true;
		// explicit value of exponent
		for (; (ndig = (*cp - '0')) <= 9; ++cp)
		{
			first = false;
			if (givenExp < MaxExponent)
				givenExp = givenExp * 10 + ndig;
		}
		if (first)
			cp = expStart;  // no exponent here, just a funny "E", but we'll accept any number we've seen so far
		givenExp *= expSign;
		mantExp += givenExp;
	}
	if (mant == 0)
		return unsigned(cp - data);
	// a double holds "53" bits in the mantissa (in a normal number the msb, 2^52, is always "1" so
	// is elided and not stored, so you get 53 bits with only 52 bits of storage)
	// 2^53-1 = 9,007,199,254,740,991 - which is almost, but not quite 16 digits.
	// if (mant >= (1ull << 53))
	//	bOverflow = true;
	if (mantExp == 0)
	{
		// short cut -- it looks like an integer, just convert it.
		// This even works for things like 10.0e+1, as long as the last digit
		// given is actually in the units place.
		outVal = static_cast<double>(mant);
		if (isNeg)
			outVal = -outVal;
	}
	else
	{
		int exp2 = Normalize(mant, mantExp);
		outVal = MakeDouble(isNeg, exp2, mant);
	}
	return unsigned(cp - data);
}
template <typename CharType>
unsigned ConvertToDouble(const CharType* data, double& outVal)
{
	return ConvertToDouble(data, outVal, '.');
}

template BASE_EXPORT unsigned int ConvertToDouble<char>(const char*, double&, char sep);
template BASE_EXPORT unsigned int ConvertToDouble<U16unit>(const U16unit*, double&, char sep);
template BASE_EXPORT unsigned int ConvertToDouble<char>(const char*, double&);
template BASE_EXPORT unsigned int ConvertToDouble<U16unit>(const U16unit*, double&);

// the purpose of these convert functions is to convert a float or double to a string with the following goals.
// 1) don't display meaningless trailing 0's or long rounds offs like 40.099998 instead of 40.01
// 2) don't use scie notation where it makes a number look less presentable
//			i.e. 0.0123 should NOT be 1.23E-002
//			exponential notation will be used when numbers would get unwieldy without it
// 3) guarantee that if you round trip the string back to the same float or double that you get the same value
//			Sometimes this means you wilt more digits in the string than are really significant.
//
// generally floats have up to 7 total significant digits and doubles 15.  Sometime it can go up to 9 & 17.
// ConvertFloatBufferSize or ConvertDoubleBufferSize should be used for the buffer sizes for these functions
BASE_EXPORT void DoubleToString::Convert(char* buffer, float f)
{
	DoubleToStringHelper::Convert<float, 7>(buffer, f);
}
BASE_EXPORT void DoubleToString::Convert(U16unit* buffer, float f)
{
	char abuffer[ConvertFloatBufferSize];
	DoubleToStringHelper::Convert<float, 7>(abuffer, f);
	for (char* p = abuffer; *p; ++p)
		*buffer++ = *p;
	*buffer++ = L'\0';
}

BASE_EXPORT void DoubleToString::Convert(char* buffer, double f)
{
	DoubleToStringHelper::Convert<double, 15>(buffer, f);
}
BASE_EXPORT void DoubleToString::Convert(U16unit* buffer, double f)
{
	char abuffer[ConvertDoubleBufferSize];
	DoubleToStringHelper::Convert<double, 15>(abuffer, f);
	for (char* p = abuffer; *p; ++p)
		*buffer++ = *p;
	*buffer++ = L'\0';
}
}  // namespace SRC
