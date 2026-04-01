// Copyright 2015, Alteryx Inc.  All rights reserved
#pragma once

#undef EXTERN_C
#ifdef __cplusplus
	#define EXTERN_C extern "C"
#else
	#define EXTERN_C
#endif

#if defined(__linux__) || defined(__APPLE__) || defined(BUILDING_SDK) || defined(BUILDING_OPEN_ALTERYX)
	#define BASE_EXPORT
	#define BASE_TEST_EXPORT
	#define BASE_E2_EXPORT
	#define BASE_E2_EXPORT_C
	#define BASE_EXPORT_CS(retType) extern "C" retType
	#define BASE_E2_EXPORT_CS(retType) extern "C" retType
#elif defined(BASE_EXPORTS) || defined(OPEN_ALTERYX_EXPORTS)
	#define BASE_EXPORT __declspec(dllexport)
	#define BASE_TEST_EXPORT __declspec(dllexport)
	#define BASE_E2_EXPORT __declspec(dllexport)
	#define BASE_E2_EXPORT_C EXTERN_C __declspec(dllexport)
	#define BASE_EXPORT_CS(retType) extern "C" __declspec(dllexport) retType __stdcall
	#define BASE_E2_EXPORT_CS(retType) extern "C" __declspec(dllexport) retType __stdcall
#elif defined(BUILDING_E2LIB)
	#define BASE_EXPORT __declspec(dllimport)
	#define BASE_TEST_EXPORT __declspec(dllimport)
	#define BASE_E2_EXPORT __declspec(dllexport)
	#define BASE_E2_EXPORT_C EXTERN_C __declspec(dllexport)
	#define BASE_EXPORT_CS(retType) extern "C" __declspec(dllimport) retType __stdcall
	#define BASE_E2_EXPORT_CS(retType) extern "C" __declspec(dllexport) retType __stdcall
#else
	#define BASE_EXPORT __declspec(dllimport)
	#define BASE_TEST_EXPORT __declspec(dllimport)
	#define BASE_E2_EXPORT __declspec(dllimport)
	#define BASE_E2_EXPORT_C EXTERN_C __declspec(dllimport)
	#define BASE_EXPORT_CS(retType) extern "C" __declspec(dllimport) retType __stdcall
	#define BASE_E2_EXPORT_CS(retType) extern "C" __declspec(dllimport) retType __stdcall
#endif
