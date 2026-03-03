use crate::persistence::model::checklist::{HeadOperation, ItemOperation};


impl Into<String> for &HeadOperation {
    fn into(self) -> String {
        use HeadOperation::*;
        match self {
            Creation { id, history, template_id, name, description } => {
                let description = description.clone().unwrap_or(String::new());
                format!(
                    "Creation {} {} {} {}:{} {}:{}\n",
                    id,
                    history,
                    template_id.map_or(String::new(), |id| id.to_string()),
                    name.matches(" ").count()+1,
                    name,
                    description.matches(" ").count()+1,
                    description,
                )
            },
            NameUpdate { id, history, head_id, name } => {
                format!(
                    "NameUpdate {} {} {} {}:{}\n",
                    id,
                    history,
                    head_id,
                    name.matches(" ").count()+1,
                    name,
                )
            },
            DescriptionUpdate { id, history, head_id, description } => {
                let description = description.clone().unwrap_or(String::new());
                format!(
                    "DescriptionUpdate {} {} {} {}:{}\n",
                    id,
                    history,
                    head_id,
                    description.matches(" ").count()+1,
                    description,
                )
            },
            CompletedUpdate { id, history, head_id, completed } => {
                format!(
                    "CompletedUpdate {} {} {} {}\n",
                    id,
                    history,
                    head_id,
                    completed,
                )
            },
            Deletion { id, history, head_id } => {
                format!(
                    "Deletion {} {} {}\n",
                    id,
                    history,
                    head_id,
                )
            },
            Tombstone { id, history, head_id, template_id, name, description, completed } => {
                let description = description.clone().unwrap_or(String::new());
                format!(
                    "Tombstone {} {} {} {} {}:{} {}:{} {}\n",
                    id,
                    history,
                    head_id,
                    template_id.map_or(String::new(), |template_id| template_id.to_string()),
                    name.matches(" ").count()+1,
                    name,
                    description.matches(" ").count()+1,
                    description,
                    completed,
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
            Creation { id, history, head_id, name, position } => {
                format!(
                    "Creation {} {} {} {}:{} {}\n",
                    id,
                    history,
                    head_id,
                    name.matches(" ").count()+1,
                    name,
                    position,
                )
            },
            NameUpdate { id, history, item_id, name } => {
                format!(
                    "NameUpdate {} {} {} {}:{}\n",
                    id,
                    history,
                    item_id,
                    name.matches(" ").count()+1,
                    name,
                )
            },
            PositionUpdate { id, history, item_id, position } => {
                format!(
                    "PositionUpdate {} {} {} {}\n",
                    id,
                    history,
                    item_id,
                    position,
                )
            },
            CheckedUpdate { id, history, item_id, checked } => {
                format!(
                    "CheckedUpdate {} {} {} {}\n",
                    id,
                    history,
                    item_id,
                    checked,
                )
            },
            Deletion { id, history, item_id } => {
                format!(
                    "Deletion {} {} {}\n",
                    id,
                    history,
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
