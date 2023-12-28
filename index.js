const fs = require('fs');
require('dotenv').config()

const { parse } = require('csv-parse/sync');
const axios = require('axios');
const algosdk = require('algosdk');
const Bottleneck = require('bottleneck');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const API_BASE_URL = 'https://api.testnet.nf.domains';

const ASSET_ID = 558991721;



const mnemonic = process.env.ALGO_WALLET_MNEMONIC;
const signerAccount = algosdk.mnemonicToSecretKey(mnemonic);
const SENDER_ADDRESS = signerAccount.addr;

// Hardcoded variables for easy editing
const algodToken = ""; // Your Algod API token
const algodServer = process.env.ALGO_ALGOD_URL; // Address of your Algod node
const algodPort = ""; // Port of your Algod node
const algodClient = new algosdk.Algodv2(algodToken, algodServer, algodPort);

// Path to the file containing the list of NFDs
const filePath = './nfd_list.txt';

const limiter = new Bottleneck({
    minTime: 200 // Roughly 300 calls per minute
});

function encodeNFDTransactionsArray(transactionsArray) {
    return transactionsArray.map(([_type, txn]) => {
      return new Uint8Array(Buffer.from(txn, 'base64'));
    });
  }

async function fetchUnsignedTransactions(payment) {
    const url = `${API_BASE_URL}/nfd/vault/sendTo/${payment.nfd}`;
    const data = {
        amount: payment.amount,
        assets: [ASSET_ID],
        sender: SENDER_ADDRESS,
        optInOnly: false,
    }
    try {
        const response = await axios.post(url, data, {
            headers: { 'Content-Type': 'application/json' }
        });
        return response.data;
    } catch (error) {
        console.error(`Error fetching transactions for ${payment.nfd}:`, error);
        return null;
    }
}

async function processNFDs(paymentList) {
    const allTransactions = [];
    for (const payment of paymentList) {
        const transactions = await limiter.schedule(() => fetchUnsignedTransactions(payment));
        const encodedTransactions = encodeNFDTransactionsArray(JSON.parse(transactions));
        allTransactions.push({ payment, encodedTransactions });
    }
    return allTransactions;
}

async function signAndSendAllTransactions(allTransactions) {
    const transactionPromises = [];

    for (const { payment, encodedTransactions } of allTransactions) {
        const unsignedTxns = encodedTransactions.map(transaction => {
            return algosdk.decodeUnsignedTransaction(transaction);
        });

        // Sign all transactions in the group
        const signedTxns = unsignedTxns.map(txn => algosdk.signTransaction(txn, signerAccount.sk).blob);

        // Create a promise for sending the group of transactions
        const transactionPromise = algodClient.sendRawTransaction(signedTxns).do()
            .then(async ({ txId }) => {
                const confirmedTxn = await algosdk.waitForConfirmation(algodClient, txId, 4);
                return { payment, success: true, txId, confirmedRound: confirmedTxn['confirmed-round'] };
            })
            .catch(error => {
                console.error('Error signing or sending transactions:', error);
                return { payment, success: false, error: error.message };
            });

        transactionPromises.push(transactionPromise);
    }

    // Wait for all transactions to complete
    const results = await Promise.all(transactionPromises);

    // Separate confirmed and failed transactions
    const confirmedTransactions = results.filter(result => result.success);
    const failedTransactions = results.filter(result => !result.success);

    return { confirmedTransactions, failedTransactions };
}




async function outputFailedTransactions(failedTransactions) {
    const csvWriter = createCsvWriter({
        path: './nfd_list-failed.csv',
        header: [
            { id: 'nfd', title: 'NFD' },
            { id: 'amount', title: 'Amount' },
            { id: 'error', title: 'Error' }
        ]
    });

    const records = failedTransactions.map(ft => ({
        nfd: ft.payment.nfd,
        amount: ft.payment.amount,
        error: ft.error
    }));

    await csvWriter.writeRecords(records);
    console.log('Failed transactions have been written to nfd_list-failed.csv');
}


// Reading the file asynchronously
fs.readFile('./nfd_list.csv', 'utf8', async (err, fileContent) => {
    if (err) {
        console.error('Error reading the file:', err);
        return;
    }
    const records = parse(fileContent, {
        skip_empty_lines: true
    });

    const paymentList = records.map(record => ({ nfd: record[0], amount: parseInt(record[1], 10) }));

    try {
        // Fetching the unsigned transactions for each NFD
        const allTransactions = await processNFDs(paymentList);

        console.log(allTransactions);
        // Sign and send all transactions and capture the results
        const { successTransactions, failedTransactions } = await signAndSendAllTransactions(allTransactions);

        console.log('Success Transactions:', successTransactions);
        
        // Check and handle if there are any failed transactions
        if (failedTransactions.length > 0) {
            console.log('Failed Transactions:', failedTransactions);
            await outputFailedTransactions(failedTransactions);
        }
    } catch (error) {
        console.error('An error occurred during processing:', error);
    }
});