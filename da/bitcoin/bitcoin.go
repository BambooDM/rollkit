package bitcoin

import (
	"bytes"
	"context"
	"fmt"

	btcec "github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/rollkit/rollkit/third_party/log"
	btctypes "github.com/rollkit/rollkit/types/pb/bitcoin"
)

// BitcoinClient interacts with Bitcoin layer
type BitcoinClient struct {
	Logger    log.Logger
	BtcClient *rpcclient.Client
}

type ResultSubmitStateProofs struct {
	Code       StatusCode
	Message    string
	SubmitHash *chainhash.Hash
}

type ResultRetrieveBlocks struct {
	Code StatusCode
	// Message may contain bitcoin specific information (like bitcoin block height/hash, detailed error message, etc)
	Message string
	Block   *wire.MsgBlock
}

// StatusCode is a type for DA layer return status.
// TODO: define an enum of different non-happy-path cases
// that might need to be handled by Rollkit independent of
// the underlying DA chain.
type StatusCode uint64

const (
	StatusSuccess StatusCode = iota
	StatusError
	StatusUnknown
	StatusStateProofsNotFound
	StatusNotFound
)

var (
	// PROTOCOL_ID allows data identification by looking at the first few bytes
	// bytes representation of "mozita"
	PROTOCOL_ID = []byte{0x6D, 0x6F, 0x7A, 0x69, 0x74, 0x61}
)

// submit state proofs to bitcoin layer
// protocol id length 6
// stateProof per block length 64
// txOrdersProofs per block length 64
// need to define the total of blocks to be committed: 0 - 9999 blocks (length 4)
func (bc *BitcoinClient) SubmitStateProofs(ctx context.Context, stateProofs btctypes.StateProofs, signerPriv string, internalKeyPriv string, networkParams *chaincfg.Params) ResultSubmitStateProofs {
	res := ResultSubmitStateProofs{}

	// how state proofs are marshalled to be stored in bitcoin layer
	var data []byte
	data = append(data, PROTOCOL_ID...)
	stateProofsBytes, err := stateProofs.Marshal()
	if err != nil {
		res.Code = StatusError
		res.Message = fmt.Sprintf("failed to marshal stateProofs with error: %s", err.Error())
		return res
	}
	data = append(data, stateProofsBytes...)

	fmt.Println("data length: ", len(data))

	// prepare taproot address for P2TR
	address, err := createTaprootAddress(data, signerPriv, internalKeyPriv)
	if err != nil {
		res.Code = StatusError
		res.Message = fmt.Sprintf("failed to create Taproot address with error: %s", err.Error())
		return res
	}

	hash, err := bc.createOutputs(address, networkParams)
	if err != nil {
		res.Code = StatusError
		res.Message = fmt.Sprintf("failed to commit transaction to the bitcoin network with error: %s", err.Error())
		return res
	}

	hash, err = bc.spendOutputs(data, hash, signerPriv, internalKeyPriv)
	if err != nil {
		res.Code = StatusError
		res.Message = fmt.Sprintf("failed to reveal transaction with error: %s", err.Error())
		return res
	}

	res.Code = StatusSuccess
	res.SubmitHash = hash

	return res
}

func (bc *BitcoinClient) RetrieveStateProofs(hash *chainhash.Hash) (*btctypes.StateProofs, error) {
	tx, err := bc.BtcClient.GetRawTransaction(hash)
	if err != nil {
		return nil, err
	}

	// if witness data is present, extract the embedded data
	stateProofs, err := bc.RetrieveStateProofsFromTx(tx.MsgTx())
	if err != nil {
		return nil, err
	}

	return stateProofs, nil
}

// try to get StateProofs from transaction data
func (bc *BitcoinClient) RetrieveStateProofsFromTx(txs ...*wire.MsgTx) (*btctypes.StateProofs, error) {
	var data []byte
	for _, tx := range txs {
		if len(tx.TxIn[0].Witness) > 1 {
			witness := tx.TxIn[0].Witness[1]
			pushData, err := extractPushData(0, witness)
			if err != nil {
				return nil, err
			}
			// skip PROTOCOL_ID
			protocol_len := len(PROTOCOL_ID)
			if pushData != nil && bytes.HasPrefix(pushData, PROTOCOL_ID) {
				data = pushData[protocol_len:]
			}
		}
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("no state proofs found in transaction")
	}

	stateProofs := &btctypes.StateProofs{}
	err := stateProofs.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal stateProofs with error: %s", err.Error())
	}

	return stateProofs, nil
}

// MaxProofSize returns the max possible proof size
func (bc *BitcoinClient) MaxProofSize(ctx context.Context) (uint64, error) {
	return 0, nil
}

// RetrieveBlocks gets bitcoin blocks
func (bc *BitcoinClient) RetrieveBlocks(ctx context.Context, btcHeight int64) ResultRetrieveBlocks {
	block_hash, err := bc.BtcClient.GetBlockHash(btcHeight)
	if err != nil {
		return ResultRetrieveBlocks{
			Code:    StatusNotFound,
			Message: fmt.Sprintf("failed to retrieve block hash at height %d with error: %s", btcHeight, err.Error()),
		}
	}

	block, err := bc.BtcClient.GetBlock(block_hash)
	if err != nil {
		return ResultRetrieveBlocks{
			Code:    StatusNotFound,
			Message: fmt.Sprintf("failed to retrieve block at height %d with error: %s", btcHeight, err.Error()),
		}
	}

	return ResultRetrieveBlocks{
		Code:  StatusSuccess,
		Block: block,
	}
}

// close shuts down the client.
func (bc *BitcoinClient) CloseRPCClient() {
	bc.BtcClient.Shutdown()
}

// create outputs on the Bitcoin network
func (bc *BitcoinClient) createOutputs(addr string, networkParams *chaincfg.Params) (*chainhash.Hash, error) {
	// Create a transaction that sends 0.001 BTC to the given address.
	address, err := btcutil.DecodeAddress(addr, networkParams)
	if err != nil {
		return nil, fmt.Errorf("error decoding recipient address: %v", err)
	}

	amount, err := btcutil.NewAmount(0.001)
	if err != nil {
		return nil, fmt.Errorf("error creating new amount: %v", err)
	}

	// who is the btc sender here?
	fmt.Printf("btc address = %s \n", address)
	hash, err := bc.BtcClient.SendToAddress(address, amount)
	if err != nil {
		return nil, fmt.Errorf("error sending to address: %v", err)
	}

	return hash, nil
}

// revealTx spends the output from the commit transaction and as part of the
// script satisfying the tapscript spend path, posts the embedded data on
// chain. It returns the hash of the reveal transaction and error, if any.
func (bc *BitcoinClient) spendOutputs(embeddedData []byte, commitHash *chainhash.Hash, signerPriv string, internalKeyPriv string) (*chainhash.Hash, error) {
	rawCommitTx, err := bc.BtcClient.GetRawTransaction(commitHash)
	if err != nil {
		return nil, fmt.Errorf("error getting raw commit tx: %v", err)
	}

	// TODO: use a better way to find our output
	var commitIndex int
	var commitOutput *wire.TxOut
	for i, out := range rawCommitTx.MsgTx().TxOut {
		if out.Value == 100000 {
			commitIndex = i
			commitOutput = out
			break
		}
	}

	privKey, err := btcutil.DecodeWIF(signerPriv)
	if err != nil {
		return nil, fmt.Errorf("error decoding bob private key: %v", err)
	}

	pubKey := privKey.PrivKey.PubKey()

	internalPrivKey, err := btcutil.DecodeWIF(internalKeyPriv)
	if err != nil {
		return nil, fmt.Errorf("error decoding internal private key: %v", err)
	}

	internalPubKey := internalPrivKey.PrivKey.PubKey()

	// Step 1: Create the Taproot script.
	tapScriptTree, tapLeaf, pkScript, err := CreateTapScriptTree(embeddedData, pubKey)
	if err != nil {
		return nil, fmt.Errorf("error creating Taproot script tree: %v", err)
	}

	ctrlBlock := tapScriptTree.LeafMerkleProofs[0].ToControlBlock(
		internalPubKey,
	)

	tapScriptRootHash := tapScriptTree.RootNode.TapHash()
	outputKey := txscript.ComputeTaprootOutputKey(
		internalPubKey, tapScriptRootHash[:],
	)
	p2trScript, err := payToTaprootScript(outputKey)
	if err != nil {
		return nil, fmt.Errorf("error building p2tr script: %v", err)
	}

	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  *rawCommitTx.Hash(),
			Index: uint32(commitIndex),
		},
	})
	txOut := &wire.TxOut{
		Value: 1e3, PkScript: p2trScript,
	}
	tx.AddTxOut(txOut)

	inputFetcher := txscript.NewCannedPrevOutputFetcher(
		commitOutput.PkScript,
		commitOutput.Value,
	)
	sigHashes := txscript.NewTxSigHashes(tx, inputFetcher)

	sig, err := txscript.RawTxInTapscriptSignature(
		tx, sigHashes, 0, txOut.Value,
		txOut.PkScript, *tapLeaf, txscript.SigHashDefault,
		privKey.PrivKey,
	)

	if err != nil {
		return nil, fmt.Errorf("error signing tapscript: %v", err)
	}

	// Now that we have the sig, we'll make a valid witness
	// including the control block.
	ctrlBlockBytes, err := ctrlBlock.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("error including control block: %v", err)
	}
	tx.TxIn[0].Witness = wire.TxWitness{
		sig, pkScript, ctrlBlockBytes,
	}

	hash, err := bc.BtcClient.SendRawTransaction(tx, false)
	if err != nil {
		return nil, fmt.Errorf("error sending reveal transaction: %v", err)
	}
	return hash, nil
}

// Construct the Taproot script with one leaf, Taproot can have many leafs
func CreateTapScriptTree(embeddedData []byte, pubKey *btcec.PublicKey) (*txscript.IndexedTapScriptTree, *txscript.TapLeaf, []byte, error) {
	builder := txscript.NewScriptBuilder()
	builder.AddOp(txscript.OP_0)
	builder.AddOp(txscript.OP_IF)
	// chunk our data into digestible 520 byte chunks
	chunks := chunkSlice(embeddedData, 520)
	for _, chunk := range chunks {
		builder.AddData(chunk)
	}
	builder.AddOp(txscript.OP_ENDIF)
	// verify data signer to prove that this data package is submitted by the signer
	builder.AddData(schnorr.SerializePubKey(pubKey))
	builder.AddOp(txscript.OP_CHECKSIG)
	pkScript, err := builder.Script()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error building script: %v", err)
	}

	// aggregate leafs to create a Taproot output key
	tapLeaf := txscript.NewBaseTapLeaf(pkScript)
	tapScriptTree := txscript.AssembleTaprootScriptTree(tapLeaf)

	return tapScriptTree, &tapLeaf, pkScript, nil
}

// createTaprootAddress returns an address committing to a Taproot script with
// a single leaf containing the spend path with the script:
// <embedded data> OP_DROP <pubkey> OP_CHECKSIG
func createTaprootAddress(embeddedData []byte, signerPriv string, internalKeyPriv string) (string, error) {
	// should be fetched from env
	privKey, err := btcutil.DecodeWIF(signerPriv)
	if err != nil {
		return "", fmt.Errorf("error decoding bob private key: %v", err)
	}

	pubKey := privKey.PrivKey.PubKey()

	// Step 1: Create the Taproot script tree.
	tapScriptTree, _, _, err := CreateTapScriptTree(embeddedData, pubKey)
	if err != nil {
		return "", fmt.Errorf("error creating Taproot script tree: %v", err)
	}

	// internal private key as key path spend
	internalPrivKey, err := btcutil.DecodeWIF(internalKeyPriv)
	if err != nil {
		return "", fmt.Errorf("error decoding internal private key: %v", err)
	}

	internalPubKey := internalPrivKey.PrivKey.PubKey()

	// Step 2: Generate the Taproot tree.
	tapScriptRootHash := tapScriptTree.RootNode.TapHash()
	outputKey := txscript.ComputeTaprootOutputKey(
		internalPubKey, tapScriptRootHash[:],
	)

	// Step 3: Generate the Bech32m address.
	address, err := btcutil.NewAddressTaproot(
		schnorr.SerializePubKey(outputKey), &chaincfg.RegressionNetParams)
	if err != nil {
		return "", fmt.Errorf("error encoding Taproot address: %v", err)
	}

	return address.String(), nil
}

// payToTaprootScript creates a pk script for a pay-to-taproot output key.
func payToTaprootScript(taprootKey *btcec.PublicKey) ([]byte, error) {
	return txscript.NewScriptBuilder().
		// OP_1 to signify SegWit v1: Taproot
		AddOp(txscript.OP_1).
		AddData(schnorr.SerializePubKey(taprootKey)).
		Script()
}

// chunkSlice splits input slice into max chunkSize length slices
func chunkSlice(slice []byte, chunkSize int) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}

	return chunks
}

// bitcoin script version, and witness field
func extractPushData(scriptVersion uint16, witness []byte) ([]byte, error) {
	type templateMatch struct {
		expectPushData bool
		maxPushDatas   int
		opcode         byte
		extractedData  []byte
	}
	var template = [6]templateMatch{
		{opcode: txscript.OP_FALSE},
		{opcode: txscript.OP_IF},
		{expectPushData: true, maxPushDatas: 10},
		{opcode: txscript.OP_ENDIF},
		{expectPushData: true, maxPushDatas: 1},
		{opcode: txscript.OP_CHECKSIG},
	}

	var templateOffset int
	tokenizer := txscript.MakeScriptTokenizer(scriptVersion, witness)
out:
	for tokenizer.Next() {
		// Not a mozita script if it has more opcodes than expected in the
		// template.
		if templateOffset >= len(template) {
			return nil, nil
		}

		// read through the script and extract the data
		op := tokenizer.Opcode()
		tplEntry := &template[templateOffset]
		if tplEntry.expectPushData {
			for i := 0; i < tplEntry.maxPushDatas; i++ {
				data := tokenizer.Data()
				if data == nil {
					break out
				}
				tplEntry.extractedData = append(tplEntry.extractedData, data...)
				tokenizer.Next()
			}
		} else if op != tplEntry.opcode {
			return nil, nil
		}

		templateOffset++
	}
	// TODO: skipping err checks
	return template[2].extractedData, nil
}
