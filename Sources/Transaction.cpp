//
// Created by Sai Neeraj Kanuri on 06/04/22.
//

#include <iostream>
#include "../Headers/Transaction.h"

using namespace std;

Transaction::Transaction(string from, string to)
{
    this->from = from;
    this->to = to;
}

string Transaction::getFrom()
{
    return this->from;
}

string Transaction::getTo()
{
    return this->to;
}
