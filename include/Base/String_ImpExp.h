#pragma once

#if defined(__linux__) || defined(__APPLE__)
	#include <cstring>
	#include <cstdarg>
	#include <ostream>
	#define DEPRECATED [[deprecated]]

	#define STRING_EXPORT
	#define __cdecl
	#define __stdcall
#else
	#define WIN32_LEAN_AND_MEAN
	#include <windows.h>
	#include <tchar.h>
	#define DEPRECATED __declspec(deprecated)
	#if defined(BUILDING_SDK) || defined(BUILDING_OPEN_ALTERYX)
		#define STRING_EXPORT
	#elif defined(BASE_EXPORTS) || defined(BUILDING_E2LIB) || defined(OPEN_ALTERYX_EXPORTS)
		#define STRING_EXPORT __declspec(dllexport)
	#else
		#define STRING_EXPORT __declspec(dllimport)
	#endif

#endif
