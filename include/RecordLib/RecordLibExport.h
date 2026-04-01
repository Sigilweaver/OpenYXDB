// Copyright(C) 2005 - 2019 Alteryx, Inc. All rights reserved.
// This file is distributed to Alteryx customers as part of the
// Software Development Kit.

#pragma once

#if defined(__linux__) || defined(__APPLE__) || defined(BUILDING_SDK) || defined(BUILDING_OPEN_ALTERYX)
	#define RECORDLIB_EXPORT_CPP
	#define E1ONLY_RECORDLIB_EXPORT_CPP
#elif defined(RECORDLIB_DLL_EXPORTS) || defined(OPEN_ALTERYX_EXPORTS)
	#define RECORDLIB_EXPORT_CPP __declspec(dllexport)
	#define E1ONLY_RECORDLIB_EXPORT_CPP __declspec(dllexport)
#elif defined(BUILDING_E2LIB)
	#define RECORDLIB_EXPORT_CPP __declspec(dllexport)
	#define E1ONLY_RECORDLIB_EXPORT_CPP
#else
	#define RECORDLIB_EXPORT_CPP __declspec(dllimport)
	#define E1ONLY_RECORDLIB_EXPORT_CPP __declspec(dllimport)
#endif
