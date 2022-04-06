//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"
#include "../Headers/Transaction.h"

#include <iostream>
#include <vector>
#include <set>
#include <unistd.h>

using namespace std;

int main(int argc, char** argv) {
    // change to read from command line / MPI ...
    // support multiple files per process
    string inputFile = "./data/eth-tx-0-999999.txt";

    // Input file reading
    Reader reader;
    reader.init(inputFile);

    vector<Transaction> transactions;

    set<string> addresses;

    while (!reader.isEofReached()){
        vector<string> txn = reader.getProcessValues();
        string from = txn.at(txn.size() - 3);
        string to = txn.at(txn.size() - 2);

        reader.populateNextLine();

        Transaction transaction(from, to);

        transactions.push_back(transaction);

//        printf("%s %s\n", transaction.getFrom().c_str(), transaction.getTo().c_str());

        addresses.insert(transaction.getFrom());
        addresses.insert(transaction.getTo());
    }

//    for (std::set<std::string>::iterator it=addresses.begin(); it!=addresses.end(); ++it)
//        std::cout << *it << "\n";

    return 0;
}
