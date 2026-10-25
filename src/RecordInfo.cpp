#include "stdafx.h"

#include "RecordLib/RecordInfo.h"

#include "RecordLib/FieldTypes.h"

#if !defined(SRCLIB_REPLACEMENT) && !defined(E2)
	#include "Base/Blob.h"
	#include "Base/crc.h"
	#include "Base/Glot.h"
#endif
#if !defined(SRCLIB_REPLACEMENT)
	#include "Base/FastOutBuffer.h"
#endif

namespace SRC {
RecordInfo::RecordInfo(RecordInfo&& o)
	: m_nFixedRecordSize(o.m_nFixedRecordSize)
	, m_bContainsVarData(o.m_bContainsVarData)
	, m_bLockIn(o.m_bLockIn)
	, m_bStrictNaming(o.m_bStrictNaming)
	, m_vFields(std::move(o.m_vFields))
	, m_vOriginalFieldNames(std::move(o.m_vOriginalFieldNames))
	, m_mapFieldNums(std::move(o.m_mapFieldNums))
	, m_nMaxFieldLen(o.m_nMaxFieldLen)
	, m_pGenericEngineBase(o.m_pGenericEngineBase)
{
	o.m_nFixedRecordSize = 0;
	o.m_bContainsVarData = false;
	o.m_pGenericEngineBase = nullptr;
}

RecordInfo& RecordInfo::operator=(const RecordInfo& o)
{
	m_nFixedRecordSize = o.m_nFixedRecordSize;
	m_bContainsVarData = o.m_bContainsVarData;
	m_vOriginalFieldNames = o.m_vOriginalFieldNames;
	m_pGenericEngineBase = o.m_pGenericEngineBase;

	m_vFields.clear();
	m_vFields.reserve(m_vFields.size());

	for (const auto& fbase : o.m_vFields)
	{
		SRC::SmartPointerRefObj<SRC::FieldBase> p = fbase->Copy();
		p->m_pGenericEngine = m_pGenericEngineBase;
		p->SetFieldPosition(static_cast<int>(m_vFields.size()));
		m_vFields.push_back(p);
	}

	m_mapFieldNums = o.m_mapFieldNums;
	m_nMaxFieldLen = o.m_nMaxFieldLen;
	m_bStrictNaming = o.m_bStrictNaming;
	m_bLockIn = o.m_bLockIn;

	return *this;
}

RecordInfo& RecordInfo::operator=(RecordInfo&& o)
{
	m_nFixedRecordSize = o.m_nFixedRecordSize;
	m_bContainsVarData = o.m_bContainsVarData;
	m_vOriginalFieldNames = std::move(o.m_vOriginalFieldNames);
	m_pGenericEngineBase = o.m_pGenericEngineBase;

	m_vFields = std::move(o.m_vFields);
	m_mapFieldNums = std::move(o.m_mapFieldNums);
	m_nMaxFieldLen = o.m_nMaxFieldLen;
	m_bStrictNaming = o.m_bStrictNaming;
	m_bLockIn = o.m_bLockIn;

	o.m_nFixedRecordSize = 0;
	o.m_bContainsVarData = false;
	o.m_pGenericEngineBase = nullptr;

	return *this;
}

void RecordInfo::SetGenericEngine(const GenericEngineBase* pGenericEngineBase)
{
	m_pGenericEngineBase = pGenericEngineBase;
	for (std::vector<SRC::SmartPointerRefObj<FieldBase>>::iterator it = m_vFields.begin(); it != m_vFields.end(); it++)
		(*it)->m_pGenericEngine = m_pGenericEngineBase;
}

/*static*/ String RecordInfo::CreateFieldXml(
	String strFieldName,
	E_FieldType ft,
	unsigned nSize /* = 0*/,
	int nScale /* = 0*/,
	String strSource /* = String()*/,
	String strDescription /* = String()*/)
{
#ifndef SRCLIB_REPLACEMENT
	typedef TFastOutBuffer<String> StringBuffer;
#else
	typedef String StringBuffer;
#endif

	String strTemp;
	// the attributes have to be in alphebetical order to facilitate compared to parsed and rebuilt xml
	StringBuffer strRet;
	if (!strDescription.IsEmpty())
	{
		strRet.Append(_U("\t<Field description=\""));
		strRet += MINIXML_NAMESPACE::MiniXmlParser::EscapeAttribute(strDescription);
		strRet.Append(_U("\" name=\""));
	}
	else
	{
		strRet.Append(_U("\t<Field name=\""));
	}

	strRet += MINIXML_NAMESPACE::MiniXmlParser::EscapeAttribute(strFieldName);

	if (ft == E_FT_FixedDecimal)
	{
		strRet.Append(_U("\" scale=\""));
		strRet += strTemp.Assign(nScale);
	}

	// some places in code naively set the length to MaxFieldLength
	// even when it is a String and the length is in characters, not bytes
	if (ft == E_FT_V_WString && nSize > MaxFieldLength / sizeof(U16unit))
		nSize = MaxFieldLength / sizeof(U16unit);
	switch (ft)
	{
		case E_FT_Bool:
		case E_FT_Byte:
		case E_FT_Int16:
		case E_FT_Int32:
		case E_FT_Int64:
		case E_FT_Float:
		case E_FT_Double:
		case E_FT_Date:
		case E_FT_Time:
		case E_FT_DateTime:
			break;  // these types don't have a size
		default:
			strRet.Append(_U("\" size=\""));
			// SrcLib Replacement doesn't know how to convert unsigned values
			// but the field size limit should fit in a signed integer anyway
			assert(nSize <= static_cast<unsigned>(std::numeric_limits<int>::max()));
#ifndef __GNUG__
			static_assert(MaxFieldLength64 <= INT_MAX, "MaxFieldLength64<=INT_MAX");
#endif
			strRet += strTemp.Assign(static_cast<int>(nSize));
	}

	if (!strSource.IsEmpty())
	{
		strRet.Append(_U("\" source=\""));
		strRet += MINIXML_NAMESPACE::MiniXmlParser::EscapeAttribute(strSource);
	}
	strRet.Append(_U("\" type=\""));
	strRet += GetNameFromFieldType(ft);
	strRet.Append(_U("\"/>\n"));
#ifndef SRCLIB_REPLACEMENT
	return strRet.GetString();
#else
	return strRet;
#endif
}

/*static*/ String RecordInfo::GetFieldXml(
	const FieldSchema& field,
	const String::TChar* pFieldNamePrefix /*=""*/,
	const String::TChar* pFieldNameSuffix /*=""*/,
	bool bIncludeSource /*= true*/)
{
	return CreateFieldXml(
		pFieldNamePrefix + field.m_strFieldName + pFieldNameSuffix,
		field.m_ft,
		field.m_nSize,
		field.m_nScale,
		bIncludeSource ? field.GetSource() : String(_U("")),
		field.GetDescription());
}

String RecordInfo::GetRecordXmlMetaData(bool bIncludeSource /*= true*/) const
{
	String strRet;
	if (m_bLockIn)
		strRet = _U("<RecordInfo LockIn=\"True\" >\n");
	else
		strRet = _U("<RecordInfo>\n");
	for (std::vector<SRC::SmartPointerRefObj<FieldBase>>::const_iterator it = m_vFields.begin(); it != m_vFields.end();
		 it++)
	{
		strRet += GetFieldXml(*it->Get(), _U(""), _U(""), bIncludeSource);
	}
	strRet += _U("</RecordInfo>\n");
	return strRet;
}

std::pair<unsigned, const SRC::FieldBase*> RecordInfo::GetFieldAndIndexByType(
	const SRC::E_FieldType pi_FieldType,
	const bool pi_bThrowOnError /*= false*/,
	unsigned nSkipToIndex /*=0*/) const
{
	const SRC::FieldBase* pField;
	unsigned idx = 0;
	T_FieldVector::const_iterator itRunner = m_vFields.begin();
	T_FieldVector::const_iterator itEnd = m_vFields.end();

	for (; idx < nSkipToIndex && itRunner != itEnd; ++idx, ++itRunner)
		;

	for (; itRunner != itEnd; ++itRunner, ++idx)
	{
		pField = (*itRunner).Get();
		if (pField->m_ft == pi_FieldType)
		{
			return std::pair<unsigned, const SRC::FieldBase*>(idx, pField);
		}
	}
	if (pi_bThrowOnError)
	{
		throw SRC::Error(XMSG("Field not found by wanted FieldType"));
	}
	return std::pair<unsigned, const SRC::FieldBase*>(0, static_cast<const SRC::FieldBase*>(NULL));
}

std::vector<const SRC::FieldBase*> RecordInfo::GetFieldsByType(const SRC::E_FieldType pi_FieldType) const
{
	std::vector<const SRC::FieldBase*> aRet;
	std::pair<unsigned, const SRC::FieldBase*> nextInfo = GetFieldAndIndexByType(pi_FieldType);
	while (nextInfo.second != NULL)
	{
		aRet.push_back(nextInfo.second);
		nextInfo = GetFieldAndIndexByType(pi_FieldType, false, nextInfo.first + 1);
	}
	return aRet;
}

unsigned RecordInfo::GetNumFieldsByType(const SRC::E_FieldType eType) const
{
	unsigned nRet = 0;
	for (T_FieldVector::const_iterator itFields = m_vFields.begin(); itFields != m_vFields.end(); itFields++)
	{
		if ((*itFields)->m_ft == eType)
			nRet++;
	}
	return nRet;
}

StringNoCase RecordInfo::ValidateFieldName(
	StringNoCase strFieldName,
	unsigned nFieldNum,
	bool bIssueWarnings /*= true*/)
{
	// this is a key parameter, whose value is set by high level discussions.
	const int numDupsToTry = 999;

	String strFieldNameOrig = strFieldName;
	if (strFieldName.IsEmpty())
		strFieldName = _U("Field_") + String().Assign(int(NumFields() + 1));

	if (m_bStrictNaming && !iswalpha(*strFieldName) && *strFieldName != '_')
	{
		strFieldName = _U("_") + strFieldName;
	}

	strFieldName.TruncatePoints(m_nMaxFieldLen);

	if (m_bStrictNaming)
	{
		// TODO E2 unicode works very differently here
		for (String::TChar* p = strFieldName.Lock(); *p; ++p)
		{
			if (*p > 255 || !isalnum(*p))
				*p = L'_';
		}
		strFieldName.Unlock();
	}

	if (m_vOriginalFieldNames.size() <= nFieldNum)
		m_vOriginalFieldNames.resize(nFieldNum + 1);
	m_vOriginalFieldNames[nFieldNum] = strFieldName;

	int nNextNum = 1;
	int nDups = 0;  // keep count of generated names so we can quit after N regardless of the digit string added
	StringNoCase strRoot, strDigits;
	bool multiDigitOk = false, append9 = false;
	// Sorry about the complexity of the code here. The goal of my rewrite was to preserve the
	// prior behavior for at least the first 9 renames, but to change from adding a series
	// of "_9_9_9" to settle down to adding an underscore and just one digit string. The
	// other new thing was to add the ability to just quit after too many renames.
	// Profiling showed that the find and insert below were pain points so FieldNameUniquify gives us a fast hash of the field name
	FieldNameUniquify fnu(strFieldName);
	for (; m_mapFieldNums.find(fnu) != m_mapFieldNums.end(); fnu = strFieldName)
	{
		if (nDups == 0)
		{
			unsigned nPreviousField = m_mapFieldNums[fnu];
			// the original field name here conflicts with another, is it a rename?
			if (strFieldName != m_vOriginalFieldNames[nPreviousField])
			{
				// it was a rename, since this is an original field name, it gets to keep it.
				m_mapFieldNums.insert(std::make_pair(fnu, nFieldNum));

				// and we rename the old field (again)
				m_vFields[nPreviousField]->SetFieldName(
					ValidateFieldName(m_vOriginalFieldNames[nPreviousField], nPreviousField, false));

				return strFieldName;
			}
			strRoot = strFieldName;
			unsigned nFieldNumLen = strFieldName.Length();
			if (nFieldNumLen >= 2 && !iswdigit(strFieldName[nFieldNumLen - 2]) && '2' <= strFieldName[nFieldNumLen - 1]
				&& strFieldName[nFieldNumLen - 1] <= '8')
			{
				// the name we're modifying ends in a single digit. Let's continue the sequence
				nNextNum = strFieldName[nFieldNumLen - 1] - '0';
				strRoot.Truncate(nFieldNumLen - 1);
				append9 = true;
			}
			else if (iswdigit(strFieldName[nFieldNumLen - 1]))
			{
				// doesn't end in just a single digit, so add '_' to separate it
				strRoot += '_';
				multiDigitOk = true;
				nNextNum = 1;  // we're about to increment it to "2"
			}
			else
			{
				// doesn't end in a digit at all, so we'll add a digit string right after it
				nNextNum = 1;  // we're about to increment it to "2"
				append9 = true;
			}
		}
		if ((++nDups) >= numDupsToTry)
		{
			throw SRC::Error(ErrorKind::Schema, XMSG(
				"Could not generate a unique name for field <@1>, number @2, after attempting @3 generated names",
				strFieldNameOrig,
				String(int(nFieldNum + 1u)),
				String(int(numDupsToTry))));
		}
		++nNextNum;
		if (nNextNum == 10 && !multiDigitOk)
		{
			nNextNum = 2;
			if (append9)
			{
				// because I truncated that off to add the number, or I was appending a digit
				// until I got to 9
				strRoot += '9';
			}
			strRoot += '_';
			multiDigitOk = true;
		}
		strFieldName.Assign(strRoot.c_str(), strRoot.Length());
		strDigits.Assign(nNextNum);
		strFieldName.Truncate(m_nMaxFieldLen - strDigits.Length());
		strFieldName += strDigits;
	}

	if (bIssueWarnings && nDups > 0 && m_pGenericEngineBase != NULL)
	{
		m_pGenericEngineBase->OutputMessage(
			GenericEngineBase::MT_Warning,
			XMSG("There were multiple fields named \"@1\".  The duplicate was renamed.", strFieldNameOrig));
	}
	m_mapFieldNums.insert(std::make_pair(fnu, nFieldNum));
	return fnu.m_strFieldName;
}

const FieldBase* RecordInfo::AddField(const FieldSchema& fieldSchema, bool bIssueWarnings /* = true*/)
{
	SRC::SmartPointerRefObj<FieldBase> pField;
	switch (fieldSchema.m_ft)
	{
		case E_FT_Bool:
			pField = new Field_Bool(fieldSchema);
			break;
		case E_FT_Byte:
			pField = new Field_Byte(fieldSchema);
			break;
		case E_FT_Int16:
			pField = new Field_Int16(fieldSchema);
			break;
		case E_FT_Int32:
			pField = new Field_Int32(fieldSchema);
			break;
		case E_FT_Int64:
			pField = new Field_Int64(fieldSchema);
			break;
		case E_FT_FixedDecimal:
		{
			// maximum number in precision reached
			if (m_pGenericEngineBase && unsigned(fieldSchema.m_nSize) > MaxFixedDecimalPrecision)
				m_pGenericEngineBase->OutputMessage(
					GenericEngineBase::MT_Error,
					XMSG(
						"Fixed Decimal fields are limited to a precision of @1 characters including the sign and decimal point.",
						String(int(MaxFixedDecimalPrecision))));

			if (fieldSchema.m_nSize <= 0)
				throw Error(ErrorKind::Schema, XMSG("Field: \"@1\" is 0 length.", fieldSchema.GetFieldName()));

			if (fieldSchema.m_nSize < 2 && fieldSchema.m_nScale > 0)
				throw Error(ErrorKind::Schema, XMSG(
					"Field: \"@1\" has too large a scale (@2) for the precision (@3).",
					fieldSchema.GetFieldName(),
					String(fieldSchema.m_nScale),
					String((int)fieldSchema.m_nSize)));

			if (fieldSchema.m_nScale > 0
				&& unsigned(fieldSchema.m_nScale)
					   > (fieldSchema.m_nSize - 2))  // subtract 2: 1 for the sign and 1 for the decimal point
				throw Error(ErrorKind::Schema, XMSG(
					"Field: \"@1\" has too large a scale (@2) for the precision (@3).",
					fieldSchema.GetFieldName(),
					String(fieldSchema.m_nScale),
					String((int)fieldSchema.m_nSize)));

			pField = new Field_FixedDecimal(fieldSchema, fieldSchema.m_nSize, fieldSchema.m_nScale);
			break;
		}
		case E_FT_Float:
			pField = new Field_Float(fieldSchema);
			break;
		case E_FT_Double:
			pField = new Field_Double(fieldSchema);
			break;
		case E_FT_String:
			// don't prevent this here, because it can break existing flows and files.
			if (m_pGenericEngineBase && fieldSchema.m_nSize > static_cast<int>(MaxFixedLengthStringSize))
				m_pGenericEngineBase->OutputMessage(
					GenericEngineBase::MT_Warning,
					XMSG(
						"String fields are limited to @1 bytes.  Use a V_String field instead.",
						String(static_cast<int64_t>(MaxFixedLengthStringSize))));

			pField = new Field_String(fieldSchema, fieldSchema.m_nSize);
			break;
		case E_FT_WString:
			// don't prevent this here, because it can break existing flows and files.
			if (m_pGenericEngineBase && fieldSchema.m_nSize > static_cast<int>(MaxFixedLengthStringSize))
				m_pGenericEngineBase->OutputMessage(
					GenericEngineBase::MT_Warning,
					XMSG(
						"WString fields are limited to @1 bytes.  Use a V_WString field instead.",
						String(static_cast<int64_t>(MaxFixedLengthStringSize))));

			pField = new Field_WString(fieldSchema, fieldSchema.m_nSize);
			break;
		case E_FT_V_String:
			// going from 64bit to 32 bit can leave fields set to more or less than the maximum size
			// clearly any size over this threshold was going for the max, so set it for max
			if (fieldSchema.m_nSize > 250000000)
				pField = new Field_V_String(fieldSchema, MaxFieldLength);
			else
				pField = new Field_V_String(fieldSchema, fieldSchema.m_nSize);

			m_bContainsVarData = true;
			break;
		case E_FT_V_WString:
			if (fieldSchema.m_nSize > 125000000)
				pField = new Field_V_WString(fieldSchema, MaxFieldLength / sizeof(U16unit));
			else
				pField = new Field_V_WString(fieldSchema, fieldSchema.m_nSize);

			m_bContainsVarData = true;
			break;
		case E_FT_Date:
			pField = new Field_Date(fieldSchema);
			break;
		case E_FT_Time:
			pField = new Field_Time(fieldSchema);
			break;
		case E_FT_DateTime:
			pField = new Field_DateTime(fieldSchema);
			break;
		case E_FT_Blob:
			pField = new Field_Blob(fieldSchema, false);
			m_bContainsVarData = true;
			break;
		case E_FT_SpatialObj:
			pField = new Field_Blob(fieldSchema, true);
			m_bContainsVarData = true;
			break;
		case E_FT_Unknown:
			break;
	}

	AddField(pField, bIssueWarnings);
	return pField.Get();
}

void RecordInfo::AddField(SmartPointerRefObj<FieldBase> pField, bool bIssueWarnings)
{
	if (NumFields() >= unsigned(GetMaxFieldsLimit()))
	{
		throw Error(ErrorKind::Schema, XMSG(
			"Number of fields is larger than the @1 limit. Please reduce the number of fields.",
			String(GetMaxFieldsLimit())));
	}

	pField->SetFieldName(ValidateFieldName(pField->GetFieldName(), unsigned(m_vFields.size()), bIssueWarnings));

	pField->m_nOffset = m_nFixedRecordSize;
	pField->m_pGenericEngine = m_pGenericEngineBase;
	pField->SetFieldPosition(static_cast<int>(m_vFields.size()));
	m_vFields.push_back(pField);

	int64_t tsize = static_cast<int64_t>(m_nFixedRecordSize) + static_cast<int64_t>(pField->m_nRawSize);
	if (tsize > MaxFieldLength)
	{
		throw SRC::Error(ErrorKind::Schema, XMSG(
			"Record too big:  Records are limited to @1 bytes. Trying to make a record with @2 bytes, when adding field named '@3'",
			String(static_cast<int64_t>(MaxFieldLength)),
			String(tsize),
			pField->GetFieldName()));
	}
	m_nFixedRecordSize += pField->m_nRawSize;
	if (pField->m_bIsVarLength)
		m_bContainsVarData = true;
}

const FieldBase* RecordInfo::RenameField(unsigned nField, StringNoCase strNewFieldName)
{
	const FieldBase* pField = this->m_vFields[nField].Get();
	m_mapFieldNums.erase(pField->GetFieldName());
	pField->SetFieldName(ValidateFieldName(strNewFieldName, nField));
	return pField;
}

const FieldBase* RecordInfo::RenameField(StringNoCase strOldFieldName, StringNoCase strNewFieldName)
{
	return RenameField(this->GetFieldNum(strOldFieldName), strNewFieldName);
}

void RecordInfo::RenameFields(
	const std::vector<StringNoCase>& oldFieldNames,
	const std::vector<StringNoCase>& newFieldNames)
{
	assert(oldFieldNames.size() == newFieldNames.size());
	std::vector<const FieldSchema*> fieldSchemas(oldFieldNames.size());

	// erase all the old fields first so new names don't conflict with not renamed yet
	for (size_t i = 0; i < oldFieldNames.size(); ++i)
	{
		const FieldSchema* field = GetFieldByName(oldFieldNames[i]);
		fieldSchemas[i] = field;
		m_mapFieldNums.erase(field->GetFieldName());
	}

	for (size_t i = 0; i < newFieldNames.size(); ++i)
	{
		const FieldSchema* field = fieldSchemas[i];
		field->SetFieldName(ValidateFieldName(newFieldNames[i], field->GetFieldPosition()));
	}
}

void RecordInfo::SwapFieldNames(int nField1, int nField2)
{
	const FieldBase* pField1 = this->m_vFields[nField1].Get();
	const FieldBase* pField2 = this->m_vFields[nField2].Get();
	std::swap(pField1->m_strFieldName, pField2->m_strFieldName);
}

const FieldBase* RecordInfo::AddField(
	StringNoCase strName,
	E_FieldType ft,
	int nSize,
	int nScale,
	String strSource,
	String strDescription,
	bool bIssueWarnings)
{
	if (nSize < 0)
		throw Error(ErrorKind::Schema, XMSG("Field: \"@1\" is 0 length.", strName));

	return AddField(
		FieldSchema(std::move(strName), ft, unsigned(nSize), nScale, std::move(strSource), std::move(strDescription)),
		bIssueWarnings);
}

const FieldBase* RecordInfo::AddField(
	const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& tagField,
	const String::TChar* pNamePrefix /*= NULL*/,
	bool bIssueWarnings)
{
	return AddField(FieldSchema(tagField, pNamePrefix), bIssueWarnings);
}

void RecordInfo::InitFromXml(
	const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& xmlTag,
	const String::TChar* pNamePrefix /*= NULL*/,
	bool bIgnoreLockIn /* = false */)
{
	if (xmlTag.Start() == NULL)
		return;

	MINIXML_NAMESPACE::MiniXmlParser::TagInfo tagRecordInfo;
	if (!MINIXML_NAMESPACE::MiniXmlParser::FindXmlTag(xmlTag, tagRecordInfo, _U("RecordInfo")))
		return;
	if (!bIgnoreLockIn
		&& m_bLockIn != MINIXML_NAMESPACE::MiniXmlParser::GetAttributeDefault(tagRecordInfo, _U("LockIn"), false))
	{
		if (m_bLockIn)
			throw Error(XMSG("This tool can only be connected to another In-Database tool."));
		else
			throw Error(XMSG("This tool is not compatible with an In-Database workflow."));
	}

	MINIXML_NAMESPACE::MiniXmlParser::TagInfo tagField;

	// this function can be additive (i.e. called more than once)
	// so it needs to maintain the offset between calls.
	// these vars are reset in the constructor
	//		m_nFixedRecordSize = 0;
	//		m_bContainsVarData = false;
	for (bool bFoundField = MINIXML_NAMESPACE::MiniXmlParser::FindXmlTag(tagRecordInfo, tagField, _U("Field"));
		 bFoundField;
		 bFoundField = MINIXML_NAMESPACE::MiniXmlParser::FindNextXmlTag(tagRecordInfo, tagField, _U("Field")))
	{
		AddField(tagField, pNamePrefix);
	}
}

std::vector<String> RecordInfo::FieldNamesInXml(const MINIXML_NAMESPACE::MiniXmlParser::TagInfo& xmlTag)
{
	std::vector<String> names;
	if (xmlTag.Start() == NULL)
		return names;
	MINIXML_NAMESPACE::MiniXmlParser::TagInfo tagRecordInfo;
	if (!MINIXML_NAMESPACE::MiniXmlParser::FindXmlTag(xmlTag, tagRecordInfo, _U("RecordInfo")))
		return names;
	MINIXML_NAMESPACE::MiniXmlParser::TagInfo tagField;
	for (bool bFound = MINIXML_NAMESPACE::MiniXmlParser::FindXmlTag(tagRecordInfo, tagField, _U("Field")); bFound;
		 bFound = MINIXML_NAMESPACE::MiniXmlParser::FindNextXmlTag(tagRecordInfo, tagField, _U("Field")))
		names.push_back(MINIXML_NAMESPACE::MiniXmlParser::GetAttribute(tagField, _U("name"), true));
	return names;
}

SmartPointerRefObj<Record> RecordInfo::CreateRecord() const
{
	SmartPointerRefObj<Record> pRet(new Record);
	pRet->Init(m_nFixedRecordSize, m_bContainsVarData);
	return pRet;
}

bool RecordInfo::CompareSchemas(const RecordInfo& o) const
{
	if (m_vFields.size() != o.m_vFields.size())
		return false;

	for (unsigned x = 0; x < m_vFields.size(); x++)
	{
		if (!m_vFields[x]->Compare(*o.m_vFields[x]))
			return false;
	}
	return true;
}

bool RecordInfo::operator==(const RecordInfo& o) const
{
	return CompareSchemas(o);
}

bool RecordInfo::EqualTypes(const RecordInfo& o, bool bAllowAdditionalFields /*= false*/) const
{
	if (bAllowAdditionalFields)
	{
		if (m_vFields.size() > o.m_vFields.size())
			return false;
	}
	else
	{
		if (m_vFields.size() != o.m_vFields.size())
			return false;
	}
	for (unsigned x = 0; x < m_vFields.size(); x++)
	{
		if (!m_vFields[x]->EqualType(*o.m_vFields[x].Get()))
			return false;
	}
	return true;
}

// Tell if a complete RecordData could be at this place in memory
// See the resemblance between this and RecordInfo::Read
RECORDLIB_EXPORT_CPP bool RecordInfo::HasWholeRecord(const void* pstart, size_t len) const
{
	// a Record contains:
	// The fixed data
	// and if there is potentially Var Data
	//	int varDataTotalLen
	// and for each varData
	//	int/byte varDataLen
	//		varData
	if (!m_bContainsVarData)
		return size_t(m_nFixedRecordSize) <= len;
	if (m_nFixedRecordSize + sizeof(int32_t) > len)
		return false;
	const char* pvarDataTotalLen = static_cast<const char*>(pstart) + m_nFixedRecordSize;
	int varDataTotalLen = *(reinterpret_cast<const int32_t*>(pvarDataTotalLen));
	return m_nFixedRecordSize + sizeof(int32_t) + varDataTotalLen <= len;
}

#if !defined(SRCLIB_REPLACEMENT) && !defined(E2)
unsigned RecordInfo::GetHash()
{
	BlobData blob;
	unsigned nNumFields = NumFields();
	blob.Add(&nNumFields, sizeof(nNumFields));
	for (unsigned x = 0; x < nNumFields; ++x)
	{
		// expanding the m_ft and m_nScale fields to larger types is so that this
		// will produce the same hash value as was produced in earlier versions.
		// These fields were compressed to reduce the size of a FieldBase object.
		const FieldBase* pField = (*this)[x];
		int ft = pField->m_ft;
		blob.Add(&ft, sizeof(ft));
		blob.Add(&pField->m_nSize, sizeof(pField->m_nSize));
		int nScale = pField->m_nScale;
		blob.Add(&nScale, sizeof(nScale));
	}
	return CCRC32().FullCRC(blob.GetData(), unsigned(blob.GetSize()));
}
#endif

int RecordInfo::GetFieldNum(StringNoCase strField, bool bThrowError /*= true*/) const
{
	auto it = m_mapFieldNums.find(strField);
	if (it == m_mapFieldNums.end())
	{
		if (bThrowError)
			throw Error(
				ErrorKind::Schema, XMSG("The field \"@1\" is missing. Compare the tool configuration with the input stream.", strField));
		else
			return -1;
	}
	return it->second;
}

const FieldBase* RecordInfo::GetFieldByName(StringNoCase strField, bool bThrowError) const
{
	int nField = GetFieldNum(strField, bThrowError);
	if (nField < 0)
		return NULL;
	else
		return m_vFields[nField].Get();
}

/*static*/ BlobVal RecordInfo::GetVarDataValue(const RecordData* pRec, int nFieldOffset)
{
	unsigned nVarDataPos;
	memcpy(&nVarDataPos, ToCharP(pRec) + nFieldOffset, sizeof(nVarDataPos));

	BlobVal ret(0, NULL);
	// any values under 4 are pointing at invalid space and can be ignored
	// specifically a value of 1 indicates it is NULL
	if (nVarDataPos == 0)
	{
		ret.nLength = 0;
		ret.pValue = U16("");  // pointer to null, the field is empt, but not null
	}
	else if (nVarDataPos == 1)
	{
		ret.nLength = 0;
		ret.pValue = NULL;  // a NULL value
	}
	// if the high bit is set, that means that this is an extended length - up to MaxFieldLength64 (2GB)
	// if not set, but there is a length in bits 29 & 30 then we have the smal string optimization
	else if ((nVarDataPos & 0x80000000) == 0 && (nVarDataPos & 0x30000000) != 0)
	{
		// small string optimization
		// it is a 1,2 or 3 byte value packed into the len
		// this would have to change in a big endian architecture
		ret.nLength = nVarDataPos >> 28;
		ret.pValue = ToCharP(pRec) + nFieldOffset;
	}
	else
	{
		// the high bit may or may not be set.  Set only if it was bigger than MaxFieldLength32
		// not harm in stripping either way
		nVarDataPos &= 0x7fffffff;
		memcpy(&ret.nLength, ToCharP(pRec) + nFieldOffset + nVarDataPos, sizeof(ret.nLength));

		// see comment in Record::AddVarData
		int nLenLength;
		if (ret.nLength & 1)
		{
			nLenLength = sizeof(unsigned char);
			ret.nLength &= 0xff;
		}
		else
			nLenLength = sizeof(unsigned);

		ret.nLength >>= 1;

		ret.pValue = ToCharP(pRec) + nFieldOffset + nVarDataPos + nLenLength;
	}
	return ret;
}

/*static*/ void RecordInfo::SetVarDataValue(Record* pRec, int nFieldOffset, unsigned nLen, const void* pValue)
{
	unsigned nVarDataPos = 0;
	// when len is 0, we don't need to put it in the var data at all
	if (nLen != 0)
	{
		// small string optimization
		// we want to pack strings smaller than 3 bytes into the 4 byte len
		if (nLen <= 3)
		{
			// put the length in the 29th and 30th bits
			nVarDataPos = nLen << 28;
			// copy the data into the low order byes (up to 3 bytes) of the Pos
			// note this is relying on little endian.
			switch (nLen)
			{
				case 3:
					reinterpret_cast<unsigned char*>(&nVarDataPos)[2] = static_cast<const unsigned char*>(pValue)[2];
				case 2:
					reinterpret_cast<unsigned char*>(&nVarDataPos)[1] = static_cast<const unsigned char*>(pValue)[1];
				case 1:
					reinterpret_cast<unsigned char*>(&nVarDataPos)[0] = static_cast<const unsigned char*>(pValue)[0];
					break;
			}
		}
		else
		{
			nVarDataPos = pRec->AddVarData(pValue, nLen);

			nVarDataPos +=
				(pRec->m_nFixedRecordSize
				 - nFieldOffset);  // the pos is an offset from this field so we don't have to know the whole record layout

			// if the record is bigger than 256MB, it could interfere with the small string optimization above
			// so we set the high bit so it can be distinguished on the get
#ifndef __GNUG__
			static_assert(MaxFieldLength32 == 0x0fffffff, "MaxFieldLength32==0x0fffffff");
#endif
			if (nVarDataPos > MaxFieldLength32)
				nVarDataPos |= 0x80000000;
		}
	}
	else if (pValue == NULL)
		nVarDataPos = 1;  // this is a NULL value, not an empty one

	memcpy(reinterpret_cast<char*>(pRec->m_pRecord) + nFieldOffset, &nVarDataPos, sizeof(nVarDataPos));
}

size_t RecordInfo::GetRecordLen(const RecordData* pSrc) const
{
	size_t nTotalSize = m_nFixedRecordSize;
	if (m_bContainsVarData)
	{
		// make sure to add the var data len field into the copy
		nTotalSize += sizeof(int);

		int nVarDataSize;
		memcpy(&nVarDataSize, ToCharP(pSrc) + m_nFixedRecordSize, sizeof(int));
		nTotalSize += nVarDataSize;
	}
	assert(nTotalSize < MaxFieldLength);
	return nTotalSize;
}

void RecordInfo::AssertValid(const RecordData* pSrc) const
{
#ifdef _DEBUG
	GetRecordLen(pSrc);
#else
	#ifndef __GNUG__
	pSrc;
	#endif
#endif
}

size_t RecordInfo::Copy(void* pDest, size_t nAvailibleBytes, const RecordData* pSrc) const
{
	size_t nTotalSize = GetRecordLen(pSrc);
	if (nAvailibleBytes < nTotalSize)
		return 0;

	memcpy(pDest, pSrc, nTotalSize);
	return nTotalSize;
}

void RecordInfo::Copy(Record* r_pRecordDest, const RecordData* pRecordSrc) const
{
	r_pRecordDest->Reset();
	assert(r_pRecordDest->m_bContainsVarData == m_bContainsVarData);
	int nCopySize = m_nFixedRecordSize;
	if (m_bContainsVarData)
		nCopySize += sizeof(int);
	memcpy(r_pRecordDest->m_pRecord, pRecordSrc, nCopySize);
	if (m_bContainsVarData)
	{
		int nVarDataSize;
		memcpy(&nVarDataSize, ToCharP(pRecordSrc) + m_nFixedRecordSize, sizeof(int));
		r_pRecordDest->Allocate(nVarDataSize);
		memcpy(static_cast<char*>(r_pRecordDest->m_pRecord) + nCopySize, ToCharP(pRecordSrc) + nCopySize, nVarDataSize);
		r_pRecordDest->m_bVarDataLenUnset = false;
	}
}

void RecordInfo::FieldNameUniquify::ComputeHash()
{
	m_nHash = 0;
	//TODO E2 - this will no longer be case sensitive and needs to enumerate code points
	const String::TChar* p = m_strFieldName.c_str();
	unsigned len = m_strFieldName.Length();
	for (unsigned uPos = 0; uPos < len; uPos++)
	{
		m_nHash = (m_nHash >> (4)) ^ (m_nHash << 4) ^ towupper(*(p + uPos));
	}
}

RecordInfo::FieldNameUniquify::FieldNameUniquify(StringNoCase strFieldName)
	: m_nHash(0)
	, m_strFieldName(strFieldName)
{
	ComputeHash();
}

RecordInfo::FieldNameUniquify& RecordInfo::FieldNameUniquify::operator=(StringNoCase strFieldName)
{
	m_strFieldName.Assign(strFieldName.c_str(), strFieldName.Length());
	ComputeHash();
	return *this;
}

bool RecordInfo::FieldNameUniquify::operator<(const FieldNameUniquify& rhs) const
{
	return (m_nHash == rhs.m_nHash ? m_strFieldName < rhs.m_strFieldName : m_nHash < rhs.m_nHash);
}

RecordInfo::RecordInfo(unsigned nMaxFieldLen, bool bStrictNaming, const GenericEngineBase* pGenericEngineBase)
	: m_bStrictNaming(bStrictNaming)
	, m_nMaxFieldLen(nMaxFieldLen)
	, m_pGenericEngineBase(pGenericEngineBase)
{
}

// this will set the LockIn flag when producing the RecordXML and accept it when reading
void RecordInfo::SetLockIn(bool bLockIn)
{
	m_bLockIn = bLockIn;
}

RecordInfo::RecordInfo(const RecordInfo& o)
{
	*this = o;
}

const GenericEngineBase* RecordInfo::GetGenericEngine() const
{
	return m_pGenericEngineBase;
}

unsigned RecordInfo::NumFields() const
{
	return unsigned(m_vFields.size());
}

const FieldBase* RecordInfo::operator[](size_t n) const
{
	return m_vFields[n].Get();
}

void RecordInfo::ResetForLateRename(unsigned maxlen, bool bStrictNaming)
{
	m_nMaxFieldLen = maxlen;
	m_bStrictNaming = bStrictNaming;
}

unsigned long RecordInfo::GetFieldNumByType(const SRC::E_FieldType pi_FieldType, const bool pi_bThrowOnError) const
{
	return this->GetFieldAndIndexByType(pi_FieldType, pi_bThrowOnError).first;
}

const SRC::FieldBase* RecordInfo::GetFieldByType(const SRC::E_FieldType pi_FieldType, const bool pi_bThrowOnError) const
{
	return this->GetFieldAndIndexByType(pi_FieldType, pi_bThrowOnError).second;
}

/*static*/ int RecordInfo::GetMaxFieldsLimit()
{
	return MaxFieldsLimit;
}

decltype(RecordInfo::m_vFields.cbegin()) RecordInfo::begin() const
{
	return m_vFields.cbegin();
}

decltype(RecordInfo::m_vFields.begin()) RecordInfo::begin()
{
	return m_vFields.begin();
}

int RecordInfo::GetFixedRecordSize() const
{
	return m_nFixedRecordSize;
}

bool RecordInfo::ContainsVarData() const
{
	return m_bContainsVarData;
}

}  // namespace SRC
