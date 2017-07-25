#!/usr/bin/env python2.7
# first setup environment by running from bash in this directory:
# $ source env/bin/activate

# Imports the Google Cloud Client Library.
from google.cloud import spanner
from google.cloud.proto.spanner.v1 import type_pb2
import datetime
import pprint


def simple_query(database):
    # Execute a simple SQL statement.
    results = database.execute_sql('SELECT 7')

    for row in results:
        print(row)
    # [END spanner_quickstart]


def setup_accounts(database):
    """Inserts sample data into the given database.

    The database and table must already exist and can be created using
    `create_database`.
    """
    with database.batch() as batch:
        batch.insert_or_update(
            table='Customers',
            columns=('CustomerNumber', 'FirstName', 'LastName',),
            values=[
                (1, u'Marc', u'Richards'),
                (2, u'Catalina', u'Smith'),
                (3, u'Alice', u'Trentor'),
                (4, u'Lea', u'Martin'),
                (5, u'David', u'Lomond')])

        batch.insert_or_update(
            table='Accounts',
            columns=('CustomerNumber', 'AccountNumber', 'AccountType', 'Balance', 'CreationTime'),
            values=[
                (1, 1, 0, 0, datetime.datetime.utcnow()),
                (1, 2, 1, 0, datetime.datetime.utcnow()),
                (2, 3, 0, 0, datetime.datetime.utcnow()),
                (3, 4, 1, 0, datetime.datetime.utcnow()),
                (4, 5, 0, 0, datetime.datetime.utcnow()),
                 ])

        batch.delete(
            table='AccountHistory',
            keyset=spanner.KeySet(all_=True))

        batch.insert(
            table='AccountHistory',
            columns=('AccountNumber', 'Ts', 'ChangeAmount', 'Memo'),
            values=[
                (1, datetime.datetime.utcnow(), 0, 'New Account Initial Deposit'),
                (2, datetime.datetime.utcnow(), 0, 'New Account Initial Deposit'),
                (3, datetime.datetime.utcnow(), 0, 'New Account Initial Deposit'),
                (4, datetime.datetime.utcnow(), 0, 'New Account Initial Deposit'),
                (5, datetime.datetime.utcnow(), 0, 'New Account Initial Deposit'),
                 ])            

    print('Inserted data.')



def extract_single_row_to_tuple(results):
    is_ret_set = False
    for row in results:
        if is_ret_set:
            raise Exception('Encounted more than one row in results')
        ret = tuple(row)
        is_ret_set = True
    if not is_ret_set:
        raise Exception('Results are empty!')
    return ret


def extract_single_cell(results):
    return extract_single_row_to_tuple(results)[0]


def account_balance(database, account_number):
    params = {
        'account': account_number,
    }
    param_types = {
        'account': type_pb2.Type(code=type_pb2.INT64),
    }
    results = database.execute_sql(
        """SELECT Balance From Accounts@{FORCE_INDEX=UniqueAccountNumbers}
           WHERE AccountNumber=@account""",
        params=params, param_types=param_types)
    balance = extract_single_cell(results)
    print "ACCOUNT BALANCE", balance
    return balance    


def customer_balance(database, customer_number):
    params = {
        'customer': customer_number,
    }
    param_types = {
        'customer': type_pb2.Type(code=type_pb2.INT64),
    }
    results = database.execute_sql(
        """SELECT SUM(Accounts.Balance) From Accounts INNER JOIN Customers
           ON Accounts.CustomerNumber=Customers.CustomerNumber
           WHERE Customers.CustomerNumber=@customer""",
        params=params, param_types=param_types)
    balance = extract_single_cell(results)
    print "CUSTOMER BALANCE", balance
    return balance


def last_n_transactions(database, account_number, n):
    params = {
        'account': account_number,
        'num': n,
    }
    param_types = {
        'account': type_pb2.Type(code=type_pb2.INT64),
        'num': type_pb2.Type(code=type_pb2.INT64),
    }
    results = database.execute_sql(
        """SELECT Ts, ChangeAmount, Memo FROM AccountHistory
           WHERE AccountNumber=@account ORDER BY Ts DESC LIMIT @num""",
        params=params, param_types=param_types)
    ret = [row for row in results]
    print "RESULTS"
    pprint.pprint(ret)
    return ret


def deposit(database, customer_number, account_number, cents, memo=None):
    def deposit_runner(transaction):
        results = transaction.execute_sql(
            """SELECT Balance From Accounts
               WHERE AccountNumber={account_number} AND CustomerNumber={customer_number}""".format(
                account_number=account_number, customer_number=customer_number))
        old_balance = extract_single_cell(results)
        new_balance = old_balance + cents
        transaction.update(
            table='Accounts',
            columns=('CustomerNumber', 'AccountNumber', 'Balance'),
            values=[
                (customer_number, account_number, new_balance),
                 ])

        transaction.insert(
            table='AccountHistory',
            columns=('AccountNumber', 'Ts', 'ChangeAmount', 'Memo'),
            values=[
                (account_number, datetime.datetime.utcnow(), cents, memo),
                ])

    database.run_in_transaction(deposit_runner)
    print('Transaction complete.')


class RowAlreadyUpdated(Exception):
    pass


def compute_interest_for_all(database):
    def compute_interest(transaction, customer_number, account_number):
        # re-check (within the transaction) that the account has not been
        # updated for the current month
        results = transaction.execute_sql(
            """SELECT Balance,CURRENT_TIMESTAMP() FROM Accounts
               WHERE AccountNumber=@account AND   # ONLY fetch this one row!
               (LastInterestCalculation IS NULL OR
           (EXTRACT(MONTH FROM LastInterestCalculation) <> EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AND
            EXTRACT(YEAR FROM LastInterestCalculation) <> EXTRACT(YEAR FROM CURRENT_TIMESTAMP())))
               LIMIT 1""",
            params={'account': account_number},
            param_types={'account': type_pb2.Type(code=type_pb2.INT64)})

        try:
            old_balance, current_timestamp = extract_single_row_to_tuple(results)
        except:
            # An exception means that the row has already been updated.
            # Abort the transaction.
            raise RowAlreadyUpdated
        cents = int(0.01 * old_balance)  # monthly interest 1%
        new_balance = old_balance + cents
        transaction.update(
            table='Accounts',
            columns=('CustomerNumber', 'AccountNumber', 'Balance', 'LastInterestCalculation'),
            values=[
                (customer_number, account_number, new_balance, current_timestamp),
                 ])

        transaction.insert(
            table='AccountHistory',
            columns=('AccountNumber', 'Ts', 'ChangeAmount', 'Memo'),
            values=[
                (account_number, current_timestamp, cents, 'Monthly Interest'),
                ])

    while True:
        # Find any account that hasn't been updated for the current month
        # (This is done in a read-only transaction, and hence does not
        # take locks on the table)
        results = database.execute_sql(
            """SELECT CustomerNumber,AccountNumber FROM Accounts
               WHERE LastInterestCalculation IS NULL OR
           (EXTRACT(MONTH FROM LastInterestCalculation) <> EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AND
            EXTRACT(YEAR FROM LastInterestCalculation) <> EXTRACT(YEAR FROM CURRENT_TIMESTAMP()))
               LIMIT 1""")
        try:
            customer_number, account_number = extract_single_row_to_tuple(results)
        except:
            break
        try:
            database.run_in_transaction(compute_interest, customer_number, account_number)
        except RowAlreadyUpdated:
            print "Caught RowAlreadyUpdated"
            pass


def main():
    # Instantiate a client.
    spanner_client = spanner.Client()

    # Your Cloud Spanner instance ID.
    instance_id = 'curtiss-test'

    # Get a Cloud Spanner instance by ID.
    instance = spanner_client.instance(instance_id)

    # Your Cloud Spanner database ID.
    database_id = 'testbank'

    # Get a Cloud Spanner database by ID.
    database = instance.database(database_id)

    simple_query(database)
    setup_accounts(database)


    account_balance(database, 2)
    customer_balance(database, 1)
    deposit(database, 1, 1, 150, 'Dollar Fifty Deposit')
    deposit(database, 1, 2, 75)
    account_balance(database, 2)
    customer_balance(database, 1)
    last_n_transactions(database, 1, 2)
    compute_interest_for_all(database)

if __name__ == "__main__":
    main()
