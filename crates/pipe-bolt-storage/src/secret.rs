use std::collections::BTreeMap;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rand::{TryRng, rngs};
use serde::{Deserialize, Serialize};

use crate::error::StorageError;

const AES_256_KEY_BYTES: usize = 32;
const AES_GCM_NONCE_BYTES: usize = 12;
const AES_GCM_ALGORITHM: &str = "AES-256-GCM";

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EncryptedSecret {
    pub algorithm: String,
    pub key_id: String,
    pub nonce: String,
    pub ciphertext: String,
}

pub trait SecretCipher: Send + Sync + 'static {
    fn key_id(&self) -> &str;

    fn encrypt(&self, plaintext: &str, aad: &[u8]) -> Result<EncryptedSecret, StorageError>;

    fn decrypt(&self, encrypted: &EncryptedSecret, aad: &[u8]) -> Result<String, StorageError>;
}

pub struct AesGcmSecretCipher {
    key_id: String,
    cipher: Aes256Gcm,
}

impl AesGcmSecretCipher {
    pub fn from_base64_key(key_id: impl Into<String>, key_b64: &str) -> Result<Self, StorageError> {
        let key_id = key_id.into();
        validate_key_id(&key_id)?;

        let key = STANDARD
            .decode(key_b64)
            .map_err(|source| StorageError::InvalidSecretEncoding { source })?;

        if key.len() != AES_256_KEY_BYTES {
            return Err(StorageError::InvalidSecretKey {
                reason: "AES-256-GCM key must decode to 32 bytes",
            });
        }

        let cipher =
            Aes256Gcm::new_from_slice(&key).map_err(|_| StorageError::InvalidSecretKey {
                reason: "AES-256-GCM key initialization failed",
            })?;

        Ok(Self { key_id, cipher })
    }
}

impl SecretCipher for AesGcmSecretCipher {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn encrypt(&self, plaintext: &str, aad: &[u8]) -> Result<EncryptedSecret, StorageError> {
        let mut nonce_bytes = [0u8; AES_GCM_NONCE_BYTES];
        rngs::SysRng
            .try_fill_bytes(&mut nonce_bytes)
            .map_err(|_| StorageError::SecretCrypto {
                operation: "encrypt",
            })?;

        let ciphertext = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: plaintext.as_bytes(),
                    aad,
                },
            )
            .map_err(|_| StorageError::SecretCrypto {
                operation: "encrypt",
            })?;

        Ok(EncryptedSecret {
            algorithm: AES_GCM_ALGORITHM.to_owned(),
            key_id: self.key_id.clone(),
            nonce: STANDARD.encode(nonce_bytes),
            ciphertext: STANDARD.encode(ciphertext),
        })
    }

    fn decrypt(&self, encrypted: &EncryptedSecret, aad: &[u8]) -> Result<String, StorageError> {
        if encrypted.algorithm != AES_GCM_ALGORITHM || encrypted.key_id != self.key_id {
            return Err(StorageError::SecretCrypto {
                operation: "decrypt",
            });
        }

        let nonce = STANDARD
            .decode(&encrypted.nonce)
            .map_err(|source| StorageError::InvalidSecretEncoding { source })?;
        if nonce.len() != AES_GCM_NONCE_BYTES {
            return Err(StorageError::SecretCrypto {
                operation: "decrypt",
            });
        }

        let ciphertext = STANDARD
            .decode(&encrypted.ciphertext)
            .map_err(|source| StorageError::InvalidSecretEncoding { source })?;
        let plaintext = self
            .cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad,
                },
            )
            .map_err(|_| StorageError::SecretCrypto {
                operation: "decrypt",
            })?;

        String::from_utf8(plaintext).map_err(|_| StorageError::SecretCrypto {
            operation: "decrypt",
        })
    }
}

pub struct StorageKeyring {
    active_key_id: String,
    keys: BTreeMap<String, AesGcmSecretCipher>,
}

impl StorageKeyring {
    pub fn single(key_id: impl Into<String>, key_b64: &str) -> Result<Self, StorageError> {
        let key_id = key_id.into();
        Self::from_base64_keys(key_id.clone(), [(key_id, key_b64.to_owned())])
    }

    pub fn from_base64_keys<I>(
        active_key_id: impl Into<String>,
        keys: I,
    ) -> Result<Self, StorageError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let active_key_id = active_key_id.into();
        validate_key_id(&active_key_id)?;

        let mut ciphers = BTreeMap::new();
        for (key_id, key_b64) in keys {
            validate_key_id(&key_id)?;
            if ciphers.contains_key(&key_id) {
                return Err(StorageError::InvalidSecretKey {
                    reason: "duplicate storage key id",
                });
            }
            let cipher = AesGcmSecretCipher::from_base64_key(key_id.clone(), &key_b64)?;
            ciphers.insert(key_id, cipher);
        }

        if !ciphers.contains_key(&active_key_id) {
            return Err(StorageError::InvalidSecretKey {
                reason: "active key id must exist in keyring",
            });
        }

        Ok(Self {
            active_key_id,
            keys: ciphers,
        })
    }

    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    fn active_cipher(&self) -> Result<&AesGcmSecretCipher, StorageError> {
        self.keys
            .get(&self.active_key_id)
            .ok_or(StorageError::InvalidSecretKey {
                reason: "active key id must exist in keyring",
            })
    }
}

impl SecretCipher for StorageKeyring {
    fn key_id(&self) -> &str {
        self.active_key_id()
    }

    fn encrypt(&self, plaintext: &str, aad: &[u8]) -> Result<EncryptedSecret, StorageError> {
        self.active_cipher()?.encrypt(plaintext, aad)
    }

    fn decrypt(&self, encrypted: &EncryptedSecret, aad: &[u8]) -> Result<String, StorageError> {
        let cipher =
            self.keys
                .get(&encrypted.key_id)
                .ok_or_else(|| StorageError::UnknownSecretKey {
                    key_id: encrypted.key_id.clone(),
                })?;

        cipher.decrypt(encrypted, aad)
    }
}

fn validate_key_id(key_id: &str) -> Result<(), StorageError> {
    if key_id.trim().is_empty() || key_id.len() > 96 {
        return Err(StorageError::InvalidSecretKey {
            reason: "key_id must be non-empty and at most 96 bytes",
        });
    }

    if !key_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(StorageError::InvalidSecretKey {
            reason: "key_id contains unsupported characters",
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    use super::*;

    #[test]
    fn secret_keyring_decrypts_old_key_and_encrypts_with_active_key() {
        let old_key = STANDARD.encode([3u8; 32]);
        let new_key = STANDARD.encode([7u8; 32]);
        let old = AesGcmSecretCipher::from_base64_key("old", &old_key).expect("old cipher");
        let encrypted = old.encrypt("super-secret", b"context").expect("encrypt");
        let keyring = StorageKeyring::from_base64_keys(
            "new",
            [("old".to_owned(), old_key), ("new".to_owned(), new_key)],
        )
        .expect("keyring");

        let decrypted = keyring.decrypt(&encrypted, b"context").expect("decrypt");
        let reencrypted = keyring
            .encrypt("rotated-secret", b"context")
            .expect("encrypt");

        assert_eq!(decrypted, "super-secret");
        assert_eq!(reencrypted.key_id, "new");
    }
}
