package fabrictx

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric-lib-go/bccsp/utils"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"google.golang.org/protobuf/proto"
)

type Signer struct {
	priv_sk  *ecdsa.PrivateKey
	signcert []byte
	mspID    string
}

func SignerFromMSP(dir, mspID string) (Signer, error) {
	keyFiles, err := filepath.Glob(filepath.Join(dir, "keystore", "*_sk"))
	if err != nil || len(keyFiles) == 0 {
		return Signer{}, fmt.Errorf("no private key found: %w", err)
	}
	privBytes, err := os.ReadFile(keyFiles[0])
	if err != nil {
		return Signer{}, err
	}
	pk, err := parsePrivateKey(privBytes)
	if err != nil {
		return Signer{}, err
	}

	certFiles, err := filepath.Glob(filepath.Join(dir, "signcerts", "*.pem"))
	if err != nil || len(certFiles) == 0 {
		return Signer{}, fmt.Errorf("no signcert found: %w", err)
	}
	certPEM, err := os.ReadFile(certFiles[0])
	if err != nil {
		return Signer{}, err
	}

	return Signer{
		priv_sk:  pk,
		signcert: certPEM,
		mspID:    mspID,
	}, nil
}

func (s Signer) Sign(msg []byte) ([]byte, error) {
	return sign(s.priv_sk, msg)
}
func (s Signer) Verify(msg []byte, sig []byte) error {
	return VerifySignature(s.signcert, sig, msg)
}

func (s Signer) Serialize() ([]byte, error) {
	return proto.Marshal(&msp.SerializedIdentity{Mspid: s.mspID, IdBytes: s.signcert})
}

func VerifySignature(certPEM, signature, message []byte) error {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return fmt.Errorf("failed to decode PEM certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse certificate: %w", err)
	}
	pubKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("certificate is not ECDSA")
	}

	digest := sha256.Sum256(message)
	ok, err = verifyECDSA(pubKey, signature, digest[:])
	if err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}
	if !ok {
		return errors.New("invalid signature")
	}
	return nil
}

func verifyECDSA(k *ecdsa.PublicKey, signature, digest []byte) (bool, error) {
	r, s, err := utils.UnmarshalECDSASignature(signature)
	if err != nil {
		return false, fmt.Errorf("Failed unmashalling signature [%s]", err)
	}

	lowS, err := utils.IsLowS(k, s)
	if err != nil {
		return false, err
	}

	if !lowS {
		return false, fmt.Errorf("Invalid S. Must be smaller than half the order [%s][%s].", s, utils.GetCurveHalfOrdersAt(k.Curve))
	}

	return ecdsa.Verify(k, digest, r, s), nil
}

func sign(k *ecdsa.PrivateKey, message []byte) ([]byte, error) {
	digest := sha256.Sum256(message)
	r, s, err := ecdsa.Sign(rand.Reader, k, digest[:])
	if err != nil {
		return nil, err
	}

	s, err = utils.ToLowS(&k.PublicKey, s)
	if err != nil {
		return nil, err
	}

	return utils.MarshalECDSASignature(r, s)
}

func parsePrivateKey(privPEM []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(privPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM private key")
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse pkcs8 private key: %w", err)
	}
	pk, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an ECDSA private key")
	}
	return pk, nil
}
