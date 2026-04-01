#pragma once

#if defined(__linux__) || defined(__APPLE__)
	#define _stdcall
	#define __stdcall
	#define _int64 int64_t
	#define __int64 int64_t
	#define _I64_MAX INT64_MAX
	#define _I64_MIN INT64_MIN
	#define _forceinline
	#define WINAPI
using DWORD = unsigned int;
using LPVOID = void*;
using LONG = long;
using HANDLE = void*;
#endif
