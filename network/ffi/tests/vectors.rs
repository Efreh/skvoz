use skvoz_network::local_api::{Request, parse_strict_json};

#[test]
fn public_api1_positive_and_negative_vectors() {
    let vectors = parse_strict_json(include_bytes!("vectors/requests.json")).unwrap();
    for vector in vectors.as_array().unwrap() {
        let input = vector["json"].as_str().unwrap();
        let valid = vector["valid"].as_bool().unwrap();
        assert_eq!(
            Request::parse_json(input.as_bytes()).is_ok(),
            valid,
            "vector {}",
            vector["name"]
        );
    }
}
