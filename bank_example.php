<?php
namespace Google\Cloud\Samples\Spanner;
use Google\Cloud\Spanner\SpannerClient;
# Include the autoloader for libraries installed with composer
require __DIR__ . '/vendor/autoload.php';
/*
Uasge:
php ycsb.php
  --operationcount={number of operations} \
  --instance=[gcloud instance] \
  --database={database name} \
  --table={table to use} \
  --workload={workload file}

Note: all arguments above are mandatory
Note: This bnchmark script assumes that the table has a PK field named "id".

*/

$msg = "";
$arrKEYS = [];
$arrOPERATIONS = ['readproportion', 'updateproportion', 'scanproportion', 'insertproportion'];

# if zero, then don't process/use this table at all.
AGGREGATE_BALANCE_SHARDS = 16
class NegativeBalance {
	}
class RowAlreadyUpdated{
	}
class NoResults{
	}
class TooManyResults{
	}
class Unsupported{
	}

function generate_int64() {
	// Should check at some point that PHP can support such a large number.
	// Since this is used for a "bank account" number, it may behoove us to just generate a string instead.
	return rand(0, (1<<63)-1);
	}

function generate_customer_number() {
	return generate_int64();
	}

function generate_account_number() {
	return generate_int64();
	}

$CUSTOMERS = array();
$ACCOUNTS = array();

for (i = 0; i < 5; i++) {
	$CUSTOMERS[] = generate_customer_number();
	$ACCOUNTS[] = generate_account_number();
	}

function clear_tables($database) {
    $operation = $database->transaction(['singleUse' => false])
        ->deleteBatch('AccountHistory', )
        ->commit();
    $operation = $database->transaction(['singleUse' => false])
        ->deleteBatch('Accounts', )
        ->commit();
    $operation = $database->transaction(['singleUse' => false])
        ->deleteBatch('Customers', )
        ->commit();
	if ($AGGREGATE_BALANCE_SHARDS > 0) {
		$operation = $database->transaction(['singleUse' => false])
        	->deleteBatch('AggregateBalance', )
			->commit();
		}
	}

function setup_customers($database) {
	clear_tables($database);
	
	$table = "Customers";
	$values = array(
		array('CustomerNumber'=>$CUSTOMERS[0], 'FirstName'=>'Marc', 'LastName'=>'Richards'),
		array('CustomerNumber'=>$CUSTOMERS[1], 'FirstName'=>'Catalina', 'LastName'=>'Smith'),
		array('CustomerNumber'=>$CUSTOMERS[2], 'FirstName'=>'Alice', 'LastName'=>'Trentor'),
		array('CustomerNumber'=>$CUSTOMERS[3], 'FirstName'=>'Lea', 'LastName'=>'Martin'),
		array('CustomerNumber'=>$CUSTOMERS[4], 'FirstName'=>'David', 'LastName'=>'Lomond')
		);
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();

	$table = "Accounts";
	$values = array(
		array('CustomerNumber'=>$CUSTOMERS[0], 'AccountNumber'=>$ACCOUNTS[0], 'AccountType'=>0, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[1], 'AccountNumber'=>$ACCOUNTS[1], 'AccountType'=>1, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[2], 'AccountNumber'=>$ACCOUNTS[2], 'AccountType'=>0, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[3], 'AccountNumber'=>$ACCOUNTS[3], 'AccountType'=>1, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL),
		array('CustomerNumber'=>$CUSTOMERS[4], 'AccountNumber'=>$ACCOUNTS[4], 'AccountType'=>0, 'Balance'=>0, 'CreationTime'=>date(DATE_ATOM, time()), 'LastInterestCalculation'=>NULL)
		);
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();

	$table = "AccountHistory";
	$values = array();
	foreach ($ACCOUNTS as $a) {
		$values[] = array('AccountNumber'=>$a, 'Ts'=>date(DATE_ATOM, time()), 'ChangeAmount'=>0, 'Memo'=>'New Account Initial Deposit')
		}
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();

	if ($AGGREGATE_BALANCE_SHARDS > 0) {
		$table = 'AggregateBalance';
		$values = array();
		for ($i = 0; $i < $AGGREGATE_BALANCE_SHARDS; $i++) {
			$values[] = array('Shard'=>$i, 'Balance'=>0);
			}
		$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();
		}
	print "Inserted Data."
	}

function extract_single_row_to_array($results) {
	// Originally called tuple, but PHP does not support tuples, only arrays
	foreach ($results as $r) {
		return $r;
		}
	}

function extract_single_cell($results) {
	$r = extract_single_row_to_array($results);
	return $r[0];
	}

function account_balance($database, $account_number) {
	$snapshot = $database->snapshot();
	$results = $snapshot->execute("SELECT Balance 
		FROM Accounts{FORCE_INDEX=UniqueAccountNumbers} 
		WHERE AccountNumber = $account_number");
	$balance = extract_single_cell($results);
	print "Account Balance: $balance";
	return $balance;
	}

function customer_balance($database, $customer_number) {
	$snapshot = $database->snapshot();
	$results = $snapshot->execute("SELECT sum(Accounts.Balance) 
		FROM Accounts a INNER JOIN Customers c
		ON a.CustomerNumber = c.CustomerNumber
		WHERE c.CustomerNumber = $customer_number");
	$balance = extract_single_cell($results);
	print "Account Balance: $balance";
	return $balance;
	}

function last_n_transactions($database, $account_number, $n) {
	$snapshot = $database->snapshot();
	$results = $snapshot->execute("SELECT Ts, ChargeAmount, Memo
		FROM Accounts{FORCE_INDEX=UniqueAccountNumbers} 
		WHERE AccountNumber = $account_number
		LIMIT $n");
	print implode(", ", $results);
	return $results;
	}

function deposit_helper($transaction, $customer_number, $account_number, $cents, $memo, $new_balance, $timestamp) {
	$values = ['CustomerNumber'=>$customer_number, "AccountNumber"=>$account_number, "Balance"=>$new_balance];
    $table = "Accounts";
	$operation = $database->transaction(['singleUse' => false])
        ->updateBatch($table, [$values,])
        ->commit();
			$table = "AccountHistory";
	$operation = $database->transaction(['singleUse' => true])->insertBatch($table, $values)->commit();
	if ($AGGREGATE_BALANCE_SHARDS > 0) {
		$shard = rand(0, $AGGREGATE_BALANCE_SHARDS - 1);
		$snapshot = $database->snapshot();
		$results = $snapshot->execute_sql("SELECT Balance
			FROM AggregateBalance 
			WHERE Shard = $shard");
		$old_agg_balance = extract_single_cell($results);
		$new_agg_balance = $old_agg_balance + $cents;
		$table = "AggregateBalance";
		$values = array('Shard'=>$shard, 'Balance'=>$new_agg_balance)
		$operation = $database->transaction(['singleUse' => false])
			->updateBatch($table, [$values,])
			->commit();
		}
	}

function deposit($database, $customer_number, $account_number, $cents, $memo=NULL) {
	$database->runTransaction(function (Transaction $t) use ($spanner) {
		$results = $t->execute_sql("SELECT Balance From Accounts
               WHERE AccountNumber='$account_number'
               AND CustomerNumber='$customer_number'");
	   		$old_balance = extract_single_cell($results);
	   		$new_balance = $old_balance + $cents;
			if ($cents < 0 && $new_balance < 0) {
				// Catch Exception for negative balance
				}
			deposit_helper($t, $customer_number, $account_number, $cents, $memo, $new_balance, datetime.datetime.utcnow());
			// Need to fix this, for manually throwing an error in PHP.
			database.run_in_transaction(deposit_runner)
			print('Transaction complete.')
		}
	}
	
	
function compute_interest_for_account($transaction, $customer_number, $account_number, $last_interest_calculation) {
	$results = $transaction->execute_sql("SELECT Balance, CURRENT_TIMESTAMP()
					FROM Accounts
    				WHERE CustomerNumber='$customer_number'
					AND AccountNumber='$account_number'
					AND (LastInterestCalculation IS NULL 
						OR LastInterestCalculation='$last_interest_calculation'");
	list($old_balance, $current_timestamp) = extract_single_row_to_tuple($results);
	if ($old_balance == None || $current_timestamp == None) {
		#throw an exception, RowAlreadyUpdated, for NoResults
		}
		$cents = (int) (0.01 * $old_balance);
		$new_balance = $old_balance + $cents;
		deposit_helper($transaction, $customer_number, $account_number, $cents, 'Monthly Interest', $new_balance, $current_timestamp);
		$values = ['CustomerNumber'=>$customer_number, 
				"AccountNumber"=>$account_number,
				"lastInterestCalculation"=>$current_timestamp];
	    $table = "Accounts";
		$transaction->updateBatch($table, [$values,]);
	}

function compute_interest_for_all($database) {
	$batch_size = 2;
	while (TRUE) {
		$results = $database->execute_sql("SELECT CustomerNumber, AccountNumber, LastInterestCalculation 
			FROM Accounts
    		WHERE LastInterestCalculation IS NULL
			OR (EXTRACT(MONTH FROM LastInterestCalculation) <> EXTRACT(MONTH FROM CURRENT_TIMESTAMP())
			AND EXTRACT(YEAR FROM LastInterestCalculation) <> EXTRACT(YEAR FROM CURRENT_TIMESTAMP()))
    		LIMIT $batch_size");
			$zero_results = TRUE;
			// Try
			foreach ($results as $r) {
				$zero_results = FALSE;
				$database->runTransaction(function (Transaction $t) use ($spanner) {
					compute_interest_for_account($t, 
						$r['CustomerNumber'], 
						$r['AccountNumber'],
						$r['LastInterestCalculation']);
					}); 
					print "Computed interest for account {$r['AccountNumber']}.";
					// Needs to execute only if exception "Row already updated."
					print "Account {$r['AccountNumber']} already updated.";
				}
				
		}
	}

function verify_consistent_balances($database) {
	
}

function total_bank_balance($database)

?>
