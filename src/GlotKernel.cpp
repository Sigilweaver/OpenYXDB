#include "stdafx.h"

#include "Base/GlotKernel.h"

#include "Base/Hash128.h"

#ifdef SRCLIB_REPLACEMENT
	// SDK, R
	#include "sqlite3.h"
#else
	// e1/e2
	#include "Base/SqliteWrapper.h"
#endif

#include <algorithm>
#include <cstring>
#include <vector>

// from Greek, γλῶττα ‎(glôtta, “tongue, language”)
// #define ValidateKeyComputation

namespace SRC {

// pointer to the global, "main language", translator
GlotKernel* GlotKernel::s_pOneKernel{ nullptr };

namespace {
// my internal goodies.
std::string s_searchDir;
std::vector<GlotKernel*> s_Glots;  // I expect this to be so short that a map is unnecessary

// given UTF-8, compute the key
int64_t ComputeKeyUTF8(const char* text, int len) noexcept
{
	Hash128 hash;
	if (len > 0)
	{
		auto buffer = std::make_unique<char[]>(len);
		int nNewLen = 0;
		for (int cpos = 0; cpos < len; ++cpos)
		{
			if (text[cpos] == '\r')
				continue;
			buffer[nNewLen++] = text[cpos];
		}

		hash.Add(buffer.get(), nNewLen);
	}
	const int64_t res = hash.GetHash64() & ~((int64_t)0b1111 << 60);
	return res;
}

// ewww. Yeah, but I need a few char buffers in places where performance does not matter
char* strdup(const char* text)
{
	auto len = std::strlen(text);
	// malloc NEVER fails. Never. Nope.
	return static_cast<char*>(memcpy(malloc(len + 1), text, len + 1));
}

void ClearMsg(char*& errMsg)
{
	free(errMsg);
	errMsg = nullptr;
}
void SetMsg(char*& errMsg, const char* newMsg)
{
	free(errMsg);
	errMsg = strdup(newMsg);
}

sqlite3* FindAndOpenDB(char*& errMsg)
{
	std::string fname(s_searchDir);
	fname += "/Glot.dat";
	sqlite3* pMsgDb = nullptr;
	int dbStatus = sqlite3_open_v2(fname.c_str(), &pMsgDb, SQLITE_OPEN_READONLY, nullptr);
	if (dbStatus != SQLITE_OK)
	{
		SetMsg(errMsg, sqlite3_errstr(dbStatus));
		sqlite3_close_v2(pMsgDb);
		return nullptr;
	}
	return pMsgDb;
}

int CopyTextToBuffer(char* buffer, int cap, const char* text, int N)
{
	memcpy(buffer, text, std::min(cap, N));
	if (N < cap)
		buffer[N] = 0;  // null terminate if possible
	return N;           // the number it wanted to copy, not the number it did copy
}

void AppendBuf(char* buffer, int& len, int cap, const char* first, const char* pos)
{
	int nFill = static_cast<int>(pos - first);
	memcpy(buffer + len, first, std::min(nFill, cap - len));
	len += nFill;
}

void AppendBuf(char* buffer, int& len, int cap, const char* first)
{
	// strchr will return you a pointer to the null-terminator on a string.
	AppendBuf(buffer, len, cap, first, std::strchr(first, 0));
}

}  // anonymous namespace

///////////////////////////
/** on startup, tell directories where I should search for glot.dat
	* This will go ahead and set the main language from the registry or from
	* the specified override language */
/*static*/ GlotKernel& GlotKernel::SearchDir(const char* dirPath, const char* lang)
{
	if (s_searchDir != dirPath || strcmp(s_pOneKernel->mLang, lang))
	{
		s_searchDir = dirPath;
		SetMainLanguage(lang);
	}
	return *s_pOneKernel;
}

void GlotKernel::SetMainLanguage(const char* lang)
{
	if (!s_pOneKernel || std::strcmp(s_pOneKernel->mLang, lang))
	{
		for (GlotKernel* pglot : s_Glots)
		{
			if (!std::strcmp(pglot->mLang, lang))
			{
				s_pOneKernel = pglot;
				return;
			}
		}
		GlotKernel* pglot = new GlotKernel(lang);
		s_Glots.push_back(pglot);
		s_pOneKernel = pglot;
	}
}

/* Added for interfacing with R code via Rcpp */
GlotKernel::GlotKernel()
	: mLang(strdup("English"))
	, mErrMsg(nullptr)
	, mpMsgDB(nullptr)
	, mpMsgQuery(nullptr)
{
}

GlotKernel::GlotKernel(const char* lang)
	: mLang(strdup(lang))
	, mErrMsg(nullptr)
	, mpMsgDB(nullptr)
	, mpMsgQuery(nullptr)
{
	mpMsgDB = FindAndOpenDB(mErrMsg);
	if (!mpMsgDB)
		return;
	std::string query("SELECT Message from ");
	(query += mLang) += " where SpookyMsgKey=?; ";
	int dbStatus = sqlite3_prepare_v2(mpMsgDB, query.c_str(), static_cast<int>(query.length()), &mpMsgQuery, 0);
	if (dbStatus != SQLITE_OK)
	{
		SetErrorMsg(sqlite3_errstr(dbStatus));
		Close();
	}
}

void GlotKernel::SetErrorMsg(const char* msg)
{
	SetMsg(mErrMsg, msg);
}
void GlotKernel::ClearErrorMsg()
{
	ClearMsg(mErrMsg);
}

void GlotKernel::Close()
{
	if (mpMsgDB)
	{
		if (mpMsgQuery)
		{
			sqlite3_finalize(mpMsgQuery);
			mpMsgQuery = nullptr;
		}
		sqlite3_close_v2(mpMsgDB);
		mpMsgDB = nullptr;
	}
}
GlotKernel::~GlotKernel()
{
	Close();
}

int GlotKernel::XLlookupKey(int64_t kval, char* buffer, int cap) const
{
	int rc = sqlite3_bind_int64(mpMsgQuery, 1, kval);
	if (rc != SQLITE_OK)
	{
		// this is probably really bad. I saw this happen when running multi-threaded
		// without the lock. I don't know what else might provoke this.
		SetMsg(mErrMsg, sqlite3_errstr(rc));
		sqlite3_reset(mpMsgQuery);
		return 0;
	}
	rc = sqlite3_step(mpMsgQuery);
	if (rc != SQLITE_ROW)
	{
		// you just don't have that key in Glot.dat
		sqlite3_reset(mpMsgQuery);
		return 0;
	}
	const char* cptr = reinterpret_cast<const char*>(sqlite3_column_text(mpMsgQuery, 0));
	// "Strings returned by sqlite3_column_text() and sqlite3_column_text16(),
	// even empty strings, are always zero - terminated."
	// column_bytes is more expensive than a strlen on the text.
	// int len = sqlite3_column_bytes(mpMsgQuery, 0);
	int len = static_cast<int>(std::strlen(cptr));
	CopyTextToBuffer(buffer, cap, cptr, len);
	sqlite3_reset(mpMsgQuery);  // copy text before you reset and unlock, right.
	return len;
}

/** This key is N bytes of UTF-8. Translate it and return the translation in
	* UTF-8 as well, into the buffer, which has capacity cap bytes. Returns the number
	* of non-null bytes it tried to write; the message will be incomplete if the
	* returned length >= cap (equal means you actually got the whole message, but it
	* didn't have enough space to write a null terminator.) */
int GlotKernel::XL8(char* buffer, int cap, const char* msgKey, int N) const
{
	if (N <= 0)
	{
		if (cap > 0)  // you can might nullptr if you give zero cap...
			buffer[0] = '\0';
		return 0;
	}
	if (!mpMsgQuery)
	{
		// can't translate unless I have a language.
		return CopyTextToBuffer(buffer, cap, msgKey, N);
	}
	int64_t kval = ComputeKeyUTF8(msgKey, N);
	int rlen = XLlookupKey(kval, buffer, cap);
	if (rlen <= 0)
		return CopyTextToBuffer(buffer, cap, msgKey, N);
	return rlen;
}

// This key is null-terminated string. Translate it and return the translation in
// std::string encoded in UTF-8, into the buffer. Added for interfacing with R code via Rcpp
std::string GlotKernel::XL8_S(const std::string& msgKey) const
{
	// Assume translation may double the size of characters, and each character may
	// need up to 4 bytes for storage
	int N = static_cast<int>(msgKey.length());
	int cap = N * 8 + 1;
	std::vector<char> v(cap);
	char* buffer = &v[0];

	int bytes = XL8(buffer, cap, msgKey.c_str(), N);

	/* Copy the translation back to the return string */
	std::string strBuffer(buffer, bytes + 1);

	return strBuffer;
}

/*static*/ int GlotKernel::Interp(char* buffer, int cap, const char* msgKey, int nArgs, const char** args)
{
	assert(0 <= nArgs && nArgs <= 9);
	assert(buffer != msgKey);  // will destroy it when I copy in the first arg
	bool usedArg[9];
	memset(usedArg, false, 9);
	int bufLen = 0;
	const char* first = msgKey;
	const char* pos = first;
	char ch;
	while (0 != (ch = *pos))
	{
		++pos;
		if (ch == '@')
		{
			if (*pos == '@')
			{
				// two ats in a row, copy the first one, skip the second
				AppendBuf(buffer, bufLen, cap, first, pos);
				++pos;
				first = pos;
			}
			else if ('1' <= *pos && *pos <= '9')
			{
				// at followed by a digit
				AppendBuf(buffer, bufLen, cap, first, pos - 1);
				int argNum = (*pos++) - '1';  // gives 0 to nArgs-1
				first = pos;
				if (argNum < nArgs && args[argNum])
				{
					AppendBuf(buffer, bufLen, cap, args[argNum]);
					usedArg[argNum] = true;
				}
				else
				{
					// this can happen if a translated message file is corrupted, or the
					// extractor fails to validate the message string, or somehow the
					// translation call provided a null argurment.
					AppendBuf(buffer, bufLen, cap, "<Missing Arg Text ");
					AppendBuf(buffer, bufLen, cap, pos - 2, pos);
					AppendBuf(buffer, bufLen, cap, ">");
				}
			}
		}
	}
	AppendBuf(buffer, bufLen, cap, first, pos);
	for (int j = 0; j < nArgs; ++j)
	{
		if (!usedArg[j] && args[j] && *args[j])
		{
			AppendBuf(buffer, bufLen, cap, "; <Internal error, arg ");
			char argnum[] = { '@', static_cast<char>(j + '1'), '\0' };
			AppendBuf(buffer, bufLen, cap, argnum);
			AppendBuf(buffer, bufLen, cap, " unused: ");
			AppendBuf(buffer, bufLen, cap, args[j]);
			AppendBuf(buffer, bufLen, cap, ">");
		}
	}
	if (bufLen < cap)
		buffer[bufLen] = 0;
	return bufLen;
}

GlotKernel& GlotKernel::I18n()
{
	return *s_pOneKernel;
}

GlotKernel* GlotKernel::I18n_ptr()
{
	return s_pOneKernel;
}

const char* GlotKernel::ErrorState() const
{
	return mErrMsg;
}

}  // namespace SRC
