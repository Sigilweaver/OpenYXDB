// Copyright 2016, Alteryx, Inc. All rights reserved
//
// This file is distributed to Alteryx customers as part of the
// Software Development Kit.
//
#pragma once
#ifndef MiniXmlParser_Included
	#define MiniXmlParser_Included

	// this is built within AlteryxSDKBuilder with a different definition of various
	// things, like string. That must be defined before regular headers are included.
	#if defined(__linux__) || defined(__APPLE__) || defined(BUILDING_SDK) || defined(BUILDING_OPEN_ALTERYX)
		#define MINIXML_EXPORT
	#elif defined(BASE_EXPORTS) || defined(BUILDING_E2LIB) || defined(OPEN_ALTERYX_EXPORTS)
		#define MINIXML_EXPORT __declspec(dllexport)
	#else
		#define MINIXML_EXPORT __declspec(dllimport)
	#endif

	#include <cstdint>
	#include <vector>
	#ifndef SRCLIB_REPLACEMENT
		#include "Base/SRC_string.h"
		#include "Base/UString.h"
	#else
namespace SRC {
class UString;
}
	#endif

	#ifdef E2
		#define MINIXML_NAMESPACE e2
	#else
		#define MINIXML_NAMESPACE SRC
	#endif

namespace MINIXML_NAMESPACE {

///////////////////////////////////////////////////////////////////////////////
//	class MiniXmlParser
class MINIXML_EXPORT MiniXmlParser
{
public:
	struct TagInfo
	{
		friend class MiniXmlParser;

	private:
		String m_strXml;

		struct Attribute
		{
			std::pair<const SRC::String::TChar*, const SRC::String::TChar*> name;
			std::pair<const SRC::String::TChar*, const SRC::String::TChar*> value;
		};
		mutable std::vector<Attribute> m_vAttributes;
		mutable bool m_bAttributeInit;

		const SRC::String::TChar* m_pXmlStart;
		const SRC::String::TChar* m_pXmlEnd;

		void InitAttributes() const;

	public:
		MINIXML_EXPORT TagInfo(const SRC::String::TChar* pStart = nullptr, const SRC::String::TChar* pEnd = nullptr);
		MINIXML_EXPORT TagInfo(String strXml);

		MINIXML_EXPORT TagInfo& operator=(String strXml);

		MINIXML_EXPORT const SRC::String::TChar* Start() const;
		MINIXML_EXPORT const SRC::String::TChar* End() const;

		MINIXML_EXPORT const std::vector<Attribute>& Attributes() const&;
	};

	static SRC::AString EscapeAttribute(SRC::AString strVal);
	#if defined(E2)
	static UString EscapeAttribute(UString strVal);
	static SRC::WString EscapeAttribute(SRC::WString strVal);
	#else
	static String EscapeAttribute(String strVal);
	static String EscapeAttribute(const String::TChar* pVal);
	#endif
	#if !defined(E2LINUX) && !defined(E2WINDOWS)
	static SRC::AString EscapeAttribute(const char* pVal);
	#endif

	/// Replace XML escape sequences with their correct value and return the string
	/// (not just for attributes, sorry about the name)
	static SRC::WString UnescapeAttribute(SRC::WString strVal, bool bAllowCdata = false);
	static UString UnescapeAttribute(UString strVal, bool bAllowCdata = false);

	// Throwing an error when missing is a huge expense. Only do it if it is a disaster for it to be missing.
	static String GetAttribute(
		const TagInfo& xmlTag,
		const SRC::String::TChar* pAttributeName,
		bool bThrowErrorIfMissing);

	// Returns true and sets value if the attribute is set; returns false and does not touch value if attr isn't set
	static bool GetAttribute(const TagInfo& xmlTag, const SRC::String::TChar* pAttributeName, String& value);

	// you can give a nullptr for the requested tag name, and it will find the next tag.
	//       (Next, so if you have "<list><item>..." it will find "<item>", skipping over
	//       "<list>")
	//       Nullptr is DIFFERENT from requesting the tag "", which will find "<"
	//       starting at the beginning of the given text.
	// Non-standard: The tag name must immediately follow the '<', no space allowed
	// Non-standard: If you omit the requested name, it will identify tag names only
	//       containing alpha-numeric and '_', not the Xml standard for names
	// Non-standard: It assumes that characters allowed in tag names are recognized as
	//       alpha-numeric, but the standard allows for much more than that. If you are
	//       searching for "prefix" and the text has "prefix҈" it will think it found the
	//       tag you want. (U+0488 Combining Cyrilic Hundred Thousands sign)
	// Non-standard: No spaces are allowed in the close of the tag, </tname
	static bool FindXmlTag(
		const TagInfo& xmlTag,
		TagInfo& r_xmlElementTag,
		const SRC::String::TChar* pRequestedTagName);

	// Find the first child element within xmlTag. You can tell what you got by looking
	// at r_child.Start(). For example,
	//   FindFirstChildXmlTag("<l><i>abc</i>...", child)
	// you will get child.Start()
	static bool FindFirstChildXmlTag(const TagInfo& xmlTag, TagInfo& r_child);

	// parent is the parent of r_child. Change the r_child to point to the next child of
	// parent that has the given pTagName.
	static bool FindNextXmlTag(const TagInfo& parent, TagInfo& r_child, const SRC::String::TChar* pTagName);
	static void ScanForTagEnd(const SRC::String::TChar*& pXmlStart);

	// get the content of xmlTag, typically as legal XML.
	// bUnescapeValue = if true, the "inner xml" content has xml escapes removed. If you
	//          want this, you probably want GetXmlValue.
	// bTrim = if true, leading and trailing spaces and tabs are removed
	// Non-standard: this will fail if the tag has an attribute with a value containing '>'
	// for example:
	//    GetInnerXml("<l><i>abc</i></l>") -> "<i>abc</i>"
	static String GetInnerXml(const TagInfo& xmlTag, bool bUnescapeValue = true, bool bTrim = true);

	static String GetOuterXml(TagInfo xmlTag);

	// in Parent, try to find a Child with tag pTagName, and an attribute named pAttributeName,
	// and that attribute has value pAttributeValue. Tags, attribute names and values are all
	// case sensitive.
	// for example:
	// FindXmlTagWithAttribute("<l><i a='1'>one</i><i a='2'>two</i></l>", child, "i", "a", "2")
	// will return true and set child to "<i a='2'>two</i>"
	static bool FindXmlTagWithAttribute(
		const TagInfo& parent,
		TagInfo& r_child,
		const SRC::String::TChar* pTagName,
		const SRC::String::TChar* pAttributeName,
		const SRC::String::TChar* pAttributeValue);

	// these match the C# routines in SrcNetXmlUtil
	// Within TagParent, look for any descendant node named ElementName (not necessarily
	// just a child), and get its 'value' attribute. If the element or attribute is
	// missing you can throw, or you can return the default value.
	// For bool the result is true if the value of the attribute is a non-zero number
	//       OR the first character of the content is T or t
	static bool GetFromXml(
		const TagInfo& xmlTagParent,
		const SRC::String::TChar* strElementName,
		bool bDefault,
		bool bThrowErrorIfMissing = false);
	// For the numeric forms you only get the default if the element is missing. If the
	// element is present but the attribute 'value' is missing you get zero. If you ask to
	// throw if missing it throws if either the element or the attribute is missing.
	// There is no validation that the value is a proper number or in bounds. You may
	// get zero or INT_MAX or something else "interesting". Robust code should get the
	// attribute value as a string and validate it.
	static int GetFromXml(
		const TagInfo& xmlTagParent,
		const SRC::String::TChar* strElementName,
		int iDefault,
		bool bThrowErrorIfMissing = false);
	static int64_t GetFromXml(
		const TagInfo& xmlTagParent,
		const SRC::String::TChar* strElementName,
		int64_t iDefault,
		bool bThrowErrorIfMissing = false);
	static double GetFromXml(
		const TagInfo& xmlTagParent,
		const SRC::String::TChar* strElementName,
		double dDefault,
		bool bThrowErrorIfMissing = false);
	// for string, the CONTENT is given, not the value attribute, and returned as the
	// inner-xml, so if the element contains more xml nodes, you get the whole xml text.
	// However, xml escapes are expanded, so you may not be able to parse it as xml again.
	// You should use finer grained functions to get the text in that case. For example:
	//    with top="<sub><a>one</a><b>two\t</b><subsub><c>thre&lt; </c></subsub></sub>"
	//    GetFromXml(top, "b", "def") -> "<subsub><c>thre< </c></subsub>"
	static String GetFromXml(
		const TagInfo& xmlTagParent,
		const SRC::String::TChar* strElementName,
		const SRC::String::TChar* strDefault,
		bool bThrowErrorIfMissing = false);

	// can't distinguish between not set and default value. Set to blank returns blank.
	static String GetAttributeDefault(
		const TagInfo& xmlTag,
		const SRC::String::TChar* pAttributeName,
		String strDefault);

	// can't distinguish between not set, set blank, and default value.
	// No validation on the value, it just calls _wtol on it.
	static int GetAttributeDefault(const TagInfo& xmlTag, const SRC::String::TChar* pAttributeName, int nDefault);

	// can't distinguish between not set, set blank, and default value.
	// True is when the value is set and begins with 'T' or 't'. "Trumped up" is true. "Yes" and "1" are false.
	static bool GetAttributeDefault(const TagInfo& xmlTag, const SRC::String::TChar* pAttributeName, bool bDefault);
};

}  // namespace MINIXML_NAMESPACE

#endif
