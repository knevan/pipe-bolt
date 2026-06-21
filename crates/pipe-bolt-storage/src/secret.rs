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
        if key_id.trim().is_empty() {
            return Err(StorageError::InvalidSecretKey {
                reason: "key_id must not be empty",
            });
        }

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
        if encrypted.algorithm != AES_GCM_ALGORITHM {
            return Err(StorageError::SecretCrypto {
                operation: "decrypt",
            });
        }

        if encrypted.key_id != self.key_id {
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

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    use super::*;

    #[test]
    fn aes_gcm_cipher_roundtrips_secret_with_bound_context() {
        let key = STANDARD.encode([7u8; 32]);
        let cipher = AesGcmSecretCipher::from_base64_key("test", &key).expect("cipher");
        let aad = b"project:p1:broker:b1:mqtt_password";

        let encrypted = cipher.encrypt("super-secret", aad).expect("encrypt");
        let decrypted = cipher.decrypt(&encrypted, aad).expect("decrypt");

        assert_eq!(decrypted, "super-secret");
    }

    #[test]
    fn aes_gcm_cipher_rejects_wrong_context() {
        let key = STANDARD.encode([7u8; 32]);
        let cipher = AesGcmSecretCipher::from_base64_key("test", &key).expect("cipher");
        let encrypted = cipher
            .encrypt("super-secret", b"context-a")
            .expect("encrypt");

        let error = cipher
            .decrypt(&encrypted, b"context-b")
            .expect_err("decrypt error");

        assert!(matches!(error, StorageError::SecretCrypto { .. }));
    }
}
