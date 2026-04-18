//! Neon Database styled id's. Just love them so much compared to UUIDs, so much more readable
//! Neon style is {word}-{word}-{8digits}
//! Twiglet id's are {word}-{word}-{10_base62_chars}, reason being, it's such a large amount of possibilities
//! that one can safely assume there will be no collosions even with the birthday paradox
//! There are TREE_ADJECTIVES.len() * TREE_TYPES.len() * (26 + 26 + 10)^10
//! Or, in the order of many quintillions
use rand::RngExt;
use rand::distr::Alphanumeric;
pub use snowflake::ProcessUniqueId;

pub type BranchId = String;

const TREE_ADJECTIVES: &[&str] = &[
    "ancient",
    "barky",
    "branchy",
    "breezy",
    "bushy",
    "canopy",
    "dappled",
    "gnarled",
    "leafy",
    "lofty",
    "mossy",
    "rustling",
    "shady",
    "silvan",
    "spry",
    "sturdy",
    "sunlit",
    "tall",
    "verdant",
    "wild",
    "windblown",
    "woody",
    "rooted",
    "twisty",
    "whispering",
];

const TREE_TYPES: &[&str] = &[
    "oak", "maple", "cedar", "pine", "birch", "willow", "spruce", "elm", "ash", "beech", "yew",
    "fir", "redwood", "walnut", "cherry", "sycamore", "alder", "cypress", "hemlock", "poplar",
];

fn random_item(list: &'static [&'static str]) -> &'static str {
    let mut rng = rand::rng();
    list[rng.random_range(0..list.len())]
}

fn random_10_chars() -> String {
    let rng = rand::rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

fn generate_slug() -> String {
    format!(
        "{}-{}-{}",
        random_item(TREE_ADJECTIVES),
        random_item(TREE_TYPES),
        random_10_chars()
    )
}

pub fn generate_project_id() -> String {
    format!("prj-{}", generate_slug())
}

pub fn generate_branch_id() -> String {
    format!("br-{}", generate_slug())
}

pub fn generate_snapshot_id() -> String {
    format!("sc-{}", generate_slug())
}

/// process-unique time-ordered Snowflake ID for internal branch nodes
pub fn generate_node_id() -> ProcessUniqueId {
    ProcessUniqueId::new()
}
