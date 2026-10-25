#include "stdafx.h"

#include "FieldType.h"

namespace SRC {
const String& GetNameFromFieldType(E_FieldType ft)
{
	static const String s_pE_FieldTypeNames[] = {
		_U("Unknown"),  _U("Bool"),         _U("Byte"),      _U("Int16"),  _U("Int32"),
		_U("Int64"),    _U("FixedDecimal"), _U("Float"),     _U("Double"), _U("String"),
		_U("WString"),  _U("V_String"),     _U("V_WString"), _U("Date"),   _U("Time"),
		_U("DateTime"), _U("Blob"),         _U("SpatialObj")
		// GetFieldTypeFromName matches against this list
	};

	return s_pE_FieldTypeNames[ft];
}

// ASCII case-insensitive equality between a UTF-16/char name and an ASCII-only
// known name. Anything that is not an exact match, including prefixes and
// longer strings, is not equal.
static bool NameEquals(const String::TChar* p, const String& known)
{
	const String::TChar* k = known.c_str();
	for (; *k; ++k, ++p)
	{
		unsigned c = static_cast<unsigned>(*p);
		if (c >= 'A' && c <= 'Z')
			c += 'a' - 'A';
		unsigned kc = static_cast<unsigned>(*k);
		if (kc >= 'A' && kc <= 'Z')
			kc += 'a' - 'A';
		if (c != kc)
			return false;
	}
	return *p == 0;
}

E_FieldType GetFieldTypeFromName(const String::TChar* p)
{
	if (!p)
		return E_FT_Unknown;

	// Index 0 is "Unknown", which is not a valid name in a file.
	for (int ft = E_FT_Unknown + 1; ft <= E_FT_SpatialObj; ++ft)
	{
		if (NameEquals(p, GetNameFromFieldType(static_cast<E_FieldType>(ft))))
			return static_cast<E_FieldType>(ft);
	}
	return E_FT_Unknown;
}
}  // namespace SRC
