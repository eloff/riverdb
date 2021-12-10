

use crypto::md5::Md5;
use crypto::digest::Digest;

/// Construct a String hex-encoded MD5 digest of the user, password, and salt
/// According to the PostgreSQL auth algorithm.
pub fn hash_md5_password(user: &str, password: &str, salt: i32) -> String {
    let mut hasher = Md5::new();
    hasher.input_str(password);
    hasher.input_str(user);
    let mut pwd_hash = [0; 16];
    hasher.result(&mut pwd_hash);
    hasher.reset();
    hasher.input_str(&hex::encode(&pwd_hash[..]));
    hasher.input(&salt.to_be_bytes()[..]);
    hasher.result(&mut pwd_hash);

    let mut result = String::with_capacity(32+3);
    result.push_str("md5");
    result.push_str(&hex::encode(&pwd_hash[..]));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_md5_password() {
        assert_eq!(
            hash_md5_password("username", "foobar", 0xa26892c4u32 as i32),
            "md57b4e445f6041af0d6d962d0cbd830f18"
        );
        assert_eq!(
            hash_md5_password("md5_user", "password", 0x2a3d8fe0u32 as i32),
            "md562af4dd09bbb41884907a838a3233294"
        );
    }
}