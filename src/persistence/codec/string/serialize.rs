use crate::persistence::model::checklist::{HeadOperation, ItemOperation};


impl Into<String> for &HeadOperation {
    fn into(self) -> String {
        use HeadOperation::*;
        match self {
            Creation { id, itc_event, template_id, name, description } => {
                let description = description.clone().unwrap_or(String::new());
                format!(
                    "Creation {} {} {} {}:{} {}:{}\n",
                    id,
                    itc_event,
                    template_id.map_or(String::new(), |id| id.to_string()),
                    name.matches(" ").count()+1,
                    name,
                    description.matches(" ").count()+1,
                    description,
                )
            },
            NameUpdate { id, itc_event, head_id, name } => {
                format!(
                    "NameUpdate {} {} {} {}:{}\n",
                    id,
                    itc_event,
                    head_id,
                    name.matches(" ").count()+1,
                    name,
                )
            },
            DescriptionUpdate { id, itc_event, head_id, description } => {
                let description = description.clone().unwrap_or(String::new());
                format!(
                    "DescriptionUpdate {} {} {} {}:{}\n",
                    id,
                    itc_event,
                    head_id,
                    description.matches(" ").count()+1,
                    description,
                )
            },
            CompletedUpdate { id, itc_event, head_id, completed } => {
                format!(
                    "CompletedUpdate {} {} {} {}\n",
                    id,
                    itc_event,
                    head_id,
                    completed,
                )
            },
            Deletion { id, itc_event, head_id } => {
                format!(
                    "Deletion {} {} {}\n",
                    id,
                    itc_event,
                    head_id,
                )
            },
        }
    }
}

impl Into<String> for HeadOperation {
    fn into(self) -> String {
        (&self).into()
    }
}

impl Into<String> for &ItemOperation {
    fn into(self) -> String {
        use ItemOperation::*;
        match self {
            Creation { id, itc_event, head_id, name, position } => {
                format!(
                    "Creation {} {} {} {}:{} {}\n",
                    id,
                    itc_event,
                    head_id,
                    name.matches(" ").count()+1,
                    name,
                    position,
                )
            },
            NameUpdate { id, itc_event, item_id, name } => {
                format!(
                    "NameUpdate {} {} {} {}:{}\n",
                    id,
                    itc_event,
                    item_id,
                    name.matches(" ").count()+1,
                    name,
                )
            },
            PositionUpdate { id, itc_event, item_id, position } => {
                format!(
                    "PositionUpdate {} {} {} {}\n",
                    id,
                    itc_event,
                    item_id,
                    position,
                )
            },
            CheckedUpdate { id, itc_event, item_id, checked } => {
                format!(
                    "CheckedUpdate {} {} {} {}\n",
                    id,
                    itc_event,
                    item_id,
                    checked,
                )
            },
            Deletion { id, itc_event, item_id } => {
                format!(
                    "Deletion {} {} {}\n",
                    id,
                    itc_event,
                    item_id,
                )
            },
        }
    }
}

impl Into<String> for ItemOperation {
    fn into(self) -> String {
        (&self).into()
    }
}
