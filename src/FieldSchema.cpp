#include "stdafx.h"

#include "RecordLib/FieldSchema.h"

namespace SRC {
FieldSchema::FieldSchema(
	const StringNoCase& strFieldName,
	E_FieldType ft,
	unsigned nSize,
	int nScale,
	String strSource /* = String()*/,
	String strDescription /* = String()*/)
	: m_nPos(-1)
	, m_strFieldName(strFieldName)
	, m_strSource(strSource)
	, m_strDescription(strDescription)
	, m_nSize(nSize)
	, m_ft(ft)
	, m_nScale(static_cast<int16_t>(nScale))
{
}

FieldSchema::FieldSchema(const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& tagField, const String::TChar* pNamePrefix)
	: FieldSchema(FromXML(tagField, pNamePrefix))
{
}

FieldSchema::FieldSchema(const FieldSchema& fieldSchema, const StringNoCase& strNewName)
	: FieldSchema(
		strNewName,
		fieldSchema.m_ft,
		fieldSchema.m_nSize,
		fieldSchema.m_nScale,
		fieldSchema.m_strSource,
		fieldSchema.m_strDescription)
{
}

FieldSchema::FieldSchema(const FieldSchema& fieldSchema, const StringNoCase& strNewName, E_FieldType ft)
	: FieldSchema(
		strNewName,
		ft,
		fieldSchema.m_nSize,
		fieldSchema.m_nScale,
		fieldSchema.m_strSource,
		fieldSchema.m_strDescription)
{
}

FieldSchema::FieldSchema(const FieldSchema& fieldSchema, const StringNoCase& strNewName, E_FieldType ft, unsigned nSize)
	: FieldSchema(strNewName, ft, nSize, fieldSchema.m_nScale, fieldSchema.m_strSource, fieldSchema.m_strDescription)
{
}

FieldSchema::FieldSchema(
	const FieldSchema& fieldSchema,
	const StringNoCase& strNewName,
	E_FieldType ft,
	unsigned nSize,
	int nScale)
	: FieldSchema(strNewName, ft, nSize, nScale, fieldSchema.m_strSource, fieldSchema.m_strDescription)
{
}

FieldSchema::FieldSchema(
	const FieldSchema& fieldSchema,
	const StringNoCase& strNewName,
	E_FieldType ft,
	unsigned nSize,
	int nScale,
	const String& strSource)
	: FieldSchema(strNewName, ft, nSize, nScale, strSource, fieldSchema.m_strDescription)
{
}

FieldSchema::FieldSchema(const FieldSchema& otherSchema)
	: FieldSchema(
		otherSchema.m_strFieldName,
		otherSchema.m_ft,
		otherSchema.m_nSize,
		otherSchema.m_nScale,
		otherSchema.m_strSource,
		otherSchema.m_strDescription)
{
}

FieldSchema::~FieldSchema()
{
}

FieldSchema FieldSchema::FromXML(
	const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& tagField,
	const String::TChar* pNamePrefix)
{
	String strType = MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("type"), true);
	E_FieldType ft = GetFieldTypeFromName(strType);
	if (ft == E_FT_Unknown)
		throw Error(ErrorKind::Schema, XMSG("Unknown field type: @1", strType));
	String strName = MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("name"), true);
	if (pNamePrefix)
		strName = pNamePrefix + strName;
	String strSize = MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("size"), false);
	int nSize = strSize.ConvertToInt();
	if (nSize <= 0 && IsString(ft))
		throw Error(ErrorKind::Schema, XMSG("Field: \"@1\" is 0 length.", strName));
	String strSource(MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("source"), false));
	String strDescription(MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("description"), false));

	int nScale = 0;
	if (ft == E_FT_FixedDecimal)
	{
		const String::TChar* pDecimal = String::strchr(strSize, '.');
		if (pDecimal)
			nScale = StringHelper::sh_strtoi(pDecimal + 1);
		else
			nScale = MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("scale"), true).ConvertToInt();
	}
	FieldSchema fieldSchema(std::move(strName), ft, nSize, nScale, std::move(strSource), std::move(strDescription));

	return fieldSchema;
}

bool FieldSchema::Compare(const FieldSchema& o) const
{
	return m_ft == o.m_ft && m_strFieldName == o.m_strFieldName && m_nSize == o.m_nSize && m_nScale == o.m_nScale;
}

void FieldSchema::SetSource(String strSource) const
{
	m_strSource = strSource;
}

void FieldSchema::SetSource(String strScope, String strValue) const
{
	assert(String::strchr(strScope, ':') == nullptr);
	m_strSource = strScope + _U(":") + strValue;
}

void FieldSchema::SetDescription(String strDescription) const
{
	m_strDescription = strDescription;
}

void FieldSchema::SetFieldName(String str) const
{
	m_strFieldName = str;
}

void FieldSchema::SetFieldPosition(int nPos) const
{
	m_nPos = nPos;
}

StringNoCase FieldSchema::GetFieldName() const
{
	return m_strFieldName;
}

String FieldSchema::GetSource() const
{
	return m_strSource;
}

String FieldSchema::GetDescription() const
{
	return m_strDescription;
}

int FieldSchema::GetFieldPosition() const
{
	return m_nPos;
}

bool FieldSchema::operator==(const FieldSchema& o) const
{
	return Compare(o);
}

bool FieldSchema::operator!=(const FieldSchema& o) const
{
	return !(*this == o);
}

bool FieldSchema::EqualType(const FieldSchema& o) const
{
	return m_ft == o.m_ft && m_nSize == o.m_nSize && m_nScale == o.m_nScale;
}

}  // namespace SRC
