use std::error::Error;
use std::fmt::Display;
use std::num::ParseIntError;
use std::str::{ParseBoolError, Split};

use itc::{EventTree, ascii_coding};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};


#[derive(Debug)]
pub enum ErrorKind {
    NoData,
    UnexpectedEOL,
    NoLengthDelimiter,
    InvalidPrefix,
    IntPraseFailure(ParseIntError),
    UuidParseFailure(uuid::Error),
    ItcParseFailure(ascii_coding::ParseError),
    BoolParseFailure(ParseBoolError),
}

#[derive(Debug)]
pub struct ParserError {
    kind: ErrorKind,
    message: String,
}

impl ParserError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    pub fn no_data(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::NoData, message: message.into() }
    }

    pub fn unexpected_eol(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::UnexpectedEOL, message: message.into() }
    }

    pub fn no_length_delimiter(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::NoLengthDelimiter, message: message.into() }
    }

    pub fn invalid_prefix(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::InvalidPrefix, message: message.into() }
    }

    pub fn int_parse_failure(message: impl Into<String>, error: ParseIntError) -> Self {
        Self { kind: ErrorKind::IntPraseFailure(error), message: message.into() }
    }

    pub fn uuid_parse_failure(message: impl Into<String>, error: uuid::Error) -> Self {
        Self { kind: ErrorKind::UuidParseFailure(error), message: message.into() }
    }

    pub fn itc_parse_failure(message: impl Into<String>, error: ascii_coding::ParseError) -> Self {
        Self { kind: ErrorKind::ItcParseFailure(error), message: message.into() }
    }

    pub fn bool_parse_failure(message: impl Into<String>, error: ParseBoolError) -> Self {
        Self { kind: ErrorKind::BoolParseFailure(error), message: message.into() }
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ErrorKind::*;
        match self.kind {
            NoData => write!(f, "parser no data error: {}", self.message),
            UnexpectedEOL => write!(f, "parser unexpected eol error: {}", self.message),
            NoLengthDelimiter => write!(f, "parser no length delimiter error: {}", self.message),
            InvalidPrefix => write!(f, "parser invalid prefix error: {}", self.message),
            IntPraseFailure(_) => write!(f, "parser int parse error: {}", self.message),
            UuidParseFailure(_) => write!(f, "parser uuid parse error: {}", self.message),
            ItcParseFailure(_) => write!(f, "parser intc parse error: {}", self.message),
            BoolParseFailure(_) => write!(f, "parser bool parse error: {}", self.message),
        }
    }
}

impl Error for ParserError {}

struct Parser {}

impl Parser {
    fn get_next_str<'a>(
        iter: &mut Split<'a, &str>,
        expected_value: &str,
    ) -> Result<&'a str, ParserError> {
        iter.next().ok_or_else(|| ParserError::unexpected_eol(
            format!("expected {}, found end of line", expected_value)
        ))
    }

    fn get_next_string(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<String, ParserError> {
        let first = Self::get_next_str(iter, &format!("word 1 of {}", expected_value))?;

        let delimiter = ":";
        let mut words = Vec::<&str>::new();
        let (raw_count, word) = first.split_once(delimiter)
            .ok_or_else(|| ParserError::no_length_delimiter(
                format!(
                    "unable to split '{}' for length value, delimiter '{}' not found",
                    first,
                    delimiter,
                )
            ))?;
        words.push(word);

        let count = raw_count.parse::<u64>()
            .map_err(|e| ParserError::int_parse_failure(
                format!("unable to parse '{}' as word count", raw_count),
                e,
            ))?;

        for word_number in 2..=count {
            let next_word = Self::get_next_str(
                iter, &format!("word {} of {}", word_number, expected_value)
            )?;
            words.push(next_word)
        }

        Ok(words.join(" "))
    }

    fn parse_uuid(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<Uuid, ParserError> {
        Self::get_next_str(iter, expected_value)?
            .parse::<Uuid>()
            .map_err(|e| ParserError::uuid_parse_failure("failed to decode Uuid", e))
    }

    fn parse_optional_uuid(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<Option<Uuid>, ParserError> {
        let s = Self::get_next_str(iter, expected_value)?;

        if s.is_empty() {
            return Ok(None);
        }

        s.parse::<Uuid>()
            .map(Some)
            .map_err(|e| ParserError::uuid_parse_failure("failed to decode Uuid", e))
    }

    fn parse_itc_event_tree(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<EventTree, ParserError> {
        Self::get_next_str(iter, expected_value)?
            .parse::<EventTree>()
            .map_err(|e| ParserError::itc_parse_failure("failed to decode EventTree", e))
    }

    fn parse_bool(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<bool, ParserError> {
        Self::get_next_str(iter, expected_value)?
            .parse::<bool>()
            .map_err(|e| ParserError::bool_parse_failure("failed to decode bool", e))
    }

    fn parse_operation_meta(iter: &mut Split<'_, &str>) -> Result<(Uuid, EventTree), ParserError> {
        let id = Self::parse_uuid(iter, "id")?;
        let history = Self::parse_itc_event_tree(iter, "itc event tree")?;

        Ok((id, history))
    }

    fn parse_head_creation(iter: &mut Split<'_, &str>) -> Result<HeadOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let template_id = Self::parse_optional_uuid(iter, "template id")?;

        let name = Self::get_next_string(iter, "name")?;
        let description = Some(Self::get_next_string(iter, "description")?)
            .filter(|s| !s.is_empty());

        Ok(HeadOperation::Creation { id, history, template_id, name, description })
    }

    fn parse_head_name_update(iter: &mut Split<'_, &str>) -> Result<HeadOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let head_id = Self::parse_uuid(iter, "head id")?;

        let name = Self::get_next_string(iter, "name")?;

        Ok(HeadOperation::NameUpdate { id, history, head_id, name })
    }

    fn parse_head_description_update(iter: &mut Split<'_, &str>) -> Result<HeadOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let head_id = Self::parse_uuid(iter, "head id")?;

        let description = Some(Self::get_next_string(iter, "description")?)
            .filter(|s| s.is_empty());

        Ok(HeadOperation::DescriptionUpdate { id, history, head_id, description })
    }

    fn parse_head_completed_update(iter: &mut Split<'_, &str>) -> Result<HeadOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let head_id = Self::parse_uuid(iter, "head id")?;

        let completed = Self::parse_bool(iter, "completed")?;

        Ok(HeadOperation::CompletedUpdate { id, history, head_id, completed })
    }

    fn parse_head_deletion(iter: &mut Split<'_, &str>) -> Result<HeadOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let head_id = Self::parse_uuid(iter, "head id")?;

        Ok(HeadOperation::Deletion { id, history, head_id })
    }

    fn parse_head_tombstone(iter: &mut Split<'_, &str>) -> Result<HeadOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let head_id = Self::parse_uuid(iter, "head id")?;
        let template_id = Self::parse_optional_uuid(iter, "template id")?;

        let name = Self::get_next_string(iter, "name")?;
        let description = Some(Self::get_next_string(iter, "description")?)
            .filter(|s| !s.is_empty());

        let completed = Self::parse_bool(iter, "completed")?;

        Ok(HeadOperation::Tombstone { id, history, head_id, template_id, name, description, completed })
    }

    fn parse_item_creation(iter: &mut Split<'_, &str>) -> Result<ItemOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let head_id = Self::parse_uuid(iter, "head id")?;

        let name = Self::get_next_string(iter, "name")?;
        let position = Self::get_next_str(iter, "position")?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ItemOperation::Creation { id, history, head_id, name, position })
    }

    fn parse_item_name_update(iter: &mut Split<'_, &str>) -> Result<ItemOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let item_id = Self::parse_uuid(iter, "item id")?;

        let name = Self::get_next_string(iter, "name")?;

        Ok(ItemOperation::NameUpdate { id, history, item_id, name })
    }

    fn parse_item_position_update(iter: &mut Split<'_, &str>) -> Result<ItemOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let item_id = Self::parse_uuid(iter, "item id")?;

        let position = Self::get_next_str(iter, "position")?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ItemOperation::PositionUpdate { id, history, item_id, position })
    }

    fn parse_item_checked_update(iter: &mut Split<'_, &str>) -> Result<ItemOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let item_id = Self::parse_uuid(iter, "item id")?;

        let checked = Self::parse_bool(iter, "checked")?;

        Ok(ItemOperation::CheckedUpdate { id, history, item_id, checked })
    }

    fn parse_item_deletion(iter: &mut Split<'_, &str>) -> Result<ItemOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let item_id = Self::parse_uuid(iter, "item id")?;

        Ok(ItemOperation::Deletion { id, history, item_id })
    }

    fn parse_item_tombstone(iter: &mut Split<'_, &str>) -> Result<ItemOperation, ParserError> {
        let (id, history) = Self::parse_operation_meta(iter)?;
        let item_id = Self::parse_uuid(iter, "item id")?;
        let head_id = Self::parse_uuid(iter, "head id")?;
        

        let name = Self::get_next_string(iter, "name")?;

        let position = Self::get_next_str(iter, "position")?;
        let position = FractionalIndex::from_hex_string(position);

        let checked = Self::parse_bool(iter, "checked")?;

        Ok(ItemOperation::Tombstone { id, history, head_id, item_id, name, position, checked })
    }
}


impl TryFrom<&str> for HeadOperation {
    type Error = ParserError;

    fn try_from(line: &str) -> std::result::Result<Self, Self::Error> {
        let mut parts = line.split(" ");
        match parts.next() {
            Some("Creation") => Parser::parse_head_creation(&mut parts),
            Some("NameUpdate") => Parser::parse_head_name_update(&mut parts),
            Some("DescriptionUpdate") => Parser::parse_head_description_update(&mut parts),
            Some("CompletedUpdate") => Parser::parse_head_completed_update(&mut parts),
            Some("Deletion") => Parser::parse_head_deletion(&mut parts),
            Some("Tombstone") => Parser::parse_head_tombstone(&mut parts),
            Some(prefix) => Err(ParserError::invalid_prefix(format!("unexpected prefix '{}'", prefix))),
            None => Err(ParserError::no_data("no text found")),
        }
    }
}

impl TryFrom<String> for HeadOperation {
    type Error = ParserError;

    fn try_from(line: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(line.as_str())
    }
}


impl TryFrom<&str> for ItemOperation {
    type Error = ParserError;

    fn try_from(line: &str) -> Result<Self, Self::Error> {
        let mut parts = line.split(" ");
        match parts.next() {
            Some("Creation") => Parser::parse_item_creation(&mut parts),
            Some("NameUpdate") => Parser::parse_item_name_update(&mut parts),
            Some("PositionUpdate") => Parser::parse_item_position_update(&mut parts),
            Some("CheckedUpdate") => Parser::parse_item_checked_update(&mut parts),
            Some("Deletion") => Parser::parse_item_deletion(&mut parts),
            Some("Tombstone") => Parser::parse_item_tombstone(&mut parts),
            Some(prefix) => Err(ParserError::invalid_prefix(format!("unexpected prefix '{}'", prefix))),
            None => Err(ParserError::no_data("no text found")),
        }
    }
}

impl TryFrom<String> for ItemOperation {
    type Error = ParserError;

    fn try_from(line: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(line.as_str())
    }
}
