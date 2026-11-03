//! JSON merge patches (RFC 7386) between consecutive snapshots.
//!
//! A daemon's snapshot changes many times a second, mostly in small places
//! (collector health, a revision number), yet `/events` used to send the whole
//! snapshot each time. Subscribers that ask for `/events?patch=1` get the full
//! snapshot once and then only what changed.
//!
//! Merge patches cannot set a value to `null` (null deletes a key). Snapshots
//! serialise `null` only for `Option` fields, and a missing `Option` field
//! deserialises as `None`, so applying a patch reproduces the same snapshot.

use serde_json::{Map, Value};

/// The merge patch that turns `old` into `new`, or `None` when they are equal.
#[must_use]
pub fn diff(old: &Value, new: &Value) -> Option<Value> {
    match (old, new) {
        (Value::Object(before), Value::Object(after)) => {
            let mut patch = Map::new();
            for (key, value) in after {
                match before.get(key) {
                    Some(previous) => {
                        if let Some(change) = diff(previous, value) {
                            patch.insert(key.clone(), change);
                        }
                    }
                    None => {
                        patch.insert(key.clone(), value.clone());
                    }
                }
            }
            for key in before.keys() {
                if !after.contains_key(key) {
                    patch.insert(key.clone(), Value::Null);
                }
            }
            (!patch.is_empty()).then_some(Value::Object(patch))
        }
        _ if old == new => None,
        // A changed object member that became null means "removed".
        _ => Some(new.clone()),
    }
}

/// Apply a merge patch to `target` in place.
pub fn apply(target: &mut Value, patch: &Value) {
    let Value::Object(changes) = patch else {
        target.clone_from(patch);
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(members) = target else {
        unreachable!("target was just made an object");
    };
    for (key, change) in changes {
        if change.is_null() {
            members.remove(key);
        } else {
            apply(members.entry(key.clone()).or_insert(Value::Null), change);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_patch_carries_only_what_changed_and_reproduces_the_new_value() {
        let old = json!({
            "revision": 7,
            "collector": {"last_cycle_at": 100, "domains": {"a": {"state": "healthy"}, "b": {"state": "error"}}},
            "messages": {"C1": [{"ts": "1"}], "C2": [{"ts": "2"}]},
            "until": 5,
            "gone": true
        });
        let new = json!({
            "revision": 8,
            "collector": {"last_cycle_at": 101, "domains": {"a": {"state": "healthy"}, "b": {"state": "healthy"}}},
            "messages": {"C1": [{"ts": "1"}], "C2": [{"ts": "2"}, {"ts": "3"}]},
            "until": null,
            "added": [1, 2]
        });
        let patch = diff(&old, &new).unwrap();
        assert_eq!(
            patch,
            json!({
                "revision": 8,
                "collector": {"last_cycle_at": 101, "domains": {"b": {"state": "healthy"}}},
                "messages": {"C2": [{"ts": "2"}, {"ts": "3"}]},
                "until": null,
                "gone": null,
                "added": [1, 2]
            })
        );
        let mut applied = old.clone();
        apply(&mut applied, &patch);
        // `until: null` (an Option set to None) arrives as a missing key.
        let mut expected = new.clone();
        expected.as_object_mut().unwrap().remove("until");
        assert_eq!(applied, expected);
        assert_eq!(diff(&new, &new), None);
    }
}
