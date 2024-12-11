#!/usr/bin/env python3
import logging
import os
import json
import sys
import argparse
import json
import pandas as pd
import singer
from target_xero.client import XeroClient

logger = singer.get_logger()

def load_json(path):
    with open(path) as f:
        return json.load(f)


def write_json_file(filename, content):
    with open(filename, 'w') as f:
        json.dump(content, f, indent=4)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args


def load_journal_entries(config, accounts, categories):
    # Get input path
    input_path = f"{config['input_path']}/JournalEntries.csv"
    # Read the passed CSV
    df = pd.read_csv(input_path)
    # Verify it has required columns
    cols = list(df.columns)
    REQUIRED_COLS = ["Transaction Date", "Journal Entry Id", "Class",
                     "Account Number", "Account Name", "Posting Type", "Description"]

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(
            f"CSV is mising REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}")
        sys.exit(1)

    journal_entries = []
    errored = False

    def add_tracking(line_item, tracking):
        if "Tracking" in line_item:
            line_item["Tracking"].append(tracking)
        else:
            line_item["Tracking"] = [tracking]

    def build_lines(x):
        # Get the journal entry id
        je_id = x['Journal Entry Id'].iloc[0]
        logger.info(f"Converting {je_id}...")
        line_items = []

        # Create line items
        for index, row in x.iterrows():
            # Create journal entry line detail
            posting_type = row['Posting Type']
            line_amt = abs(row['Amount'])
            if posting_type.lower() == 'credit':
                line_amt = -1 * line_amt

            line_item = {
                "Description": row['Description'],
                "LineAmount": line_amt
            }

            # Get the Quickbooks Account Ref
            acct_num = str(row['Account Number'])
            acct_name = row['Account Name']
            if not acct_num and not acct_name:
                raise Exception(
                    f"Account Number and account Name are missing. Please check the details for Journal Entry {je_id}."
                )

            acct_code = accounts.get(acct_num, accounts.get(acct_name, {})).get("Code")

            if acct_code is not None:
                line_item["AccountCode"] = acct_code
            else:
                errored = True
                logger.error(
                    f"Account Name='{acct_name}' No={acct_num} not found in Xero. Verify the details for Journal Entry {je_id} or ensure the account exists for the specified tenant_id in the config file."
                )
                raise Exception(
                    f"Account Name='{acct_name}' No={acct_num} not found in Xero. Verify the details for Journal Entry {je_id} or ensure the account exists for the specified tenant_id in the config file."
                )

            # Get the Quickbooks Class Ref
            class_name = row['Class']
            tracking = categories.get(class_name)

            if tracking is not None:
                add_tracking(line_item, tracking)
            else:
                logger.warning(
                    f"Class '{class_name}' not found in Xero for Journal Entry {je_id}!")

            # Get and set department if present
            if 'department' in config and config['department'] in row.index:
                dept_name = row[config['department']]
                tracking = categories.get(dept_name)

                if tracking is not None:
                    add_tracking(line_item, tracking)

            # Get and set location if present
            if 'location' in config and config['location'] in row.index:
                location = row[config['location']]
                tracking = categories.get(location)

                if tracking is not None:
                    add_tracking(line_item, tracking)

            # Get and set customer_id if present
            if 'customer_id' in config and config['customer_id'] in row.index:
                customer_id = row[config['customer_id']]
                tracking = categories.get(customer_id)

                if tracking is not None:
                    add_tracking(line_item, tracking)

            # Get and set customer_name if present
            if 'customer_name' in config and config['customer_name'] in row.index:
                customer_name = row[config['customer_name']]
                tracking = categories.get(customer_name)

                if tracking is not None:
                    add_tracking(line_item, tracking)

            # Create the line item
            line_items.append(line_item)

        # Create the entry
        entry = {
            'Date': row['Transaction Date'],
            'Status': 'POSTED',
            'Narration': je_id,
            'JournalLines': line_items
        }

        journal_entries.append(entry)

    # Format the dates
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
    df['Transaction Date'] = df['Transaction Date'].dt.strftime('%Y-%m-%d')
    # Build the entries
    df.groupby("Journal Entry Id").apply(build_lines)

    if errored:
        raise Exception("Building Xero JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


def post_journal_entries(journals, client):
    posted_journals = []

    for journal in journals:
        try:
            # Push the journal entry
            res = client.push("Manual_Journals", journal)
            res = res.json()
            if "Type" in res and res["Type"] == "ValidationException" and "Elements" in res:
                #Log validation errors
                logger.error(f"Journal Entry validation error: {json.dumps(res['Elements'])}")
            # Add to array of posted journals
            posted_journals.append(res['ManualJournals'][0]['ManualJournalID'])
        except Exception as e:
            try:
                # raise response in error if response is available
                res = res.text
                logger.error(
                    f"Failure creating entity error=[{e}] journal=[{journal}] response=[{res.content}] status_code=[{res.status_code}]"
                )
            except:
                res = e.__str__()    
                logger.error(
                    f"Failure creating entity error=[{e}] journal=[{journal}]"
                )

            # Void all posted JEs (don't want to allow a partially successful post)
            for pje in posted_journals:
                client.push("Manual_Journals", {
                    'ManualJournalID': pje,
                    'Status': 'VOIDED'
                })

                print(f"Voided Journal Entry {pje}")

            raise Exception(f"Posting Xero JournalEntries failed! {res}")


def upload_journals(config, client):
    # Load Customers, Accounts
    acc_list = client.filter("Accounts")
    cat_list = client.filter("Tracking_Categories")

    # Process accounts
    accounts = {}

    for account in acc_list:
        if account.get("Code") is None:
            continue

        name = account['Name']
        code = account['Code']
        acc_ref = {
            'Name': name,
            'Code': code
        }
        accounts[code] = acc_ref
        accounts[name] = acc_ref

    # Process categories
    categories = {}

    for category in cat_list:
        name = category['Name']
        options = [x['Name'] for x in category['Options']]

        for option in options:
            categories[option] = {
                'Name': name,
                'Option': option
            }

    # Load Journal Entries CSV to post + Convert to Xero format
    journals = load_journal_entries(config, accounts, categories)
    logger.info(json.dumps(journals))

    # Post the journal entries to Xero
    post_journal_entries(journals, client)


def upload_transactions(config, client):

    input_path = f"{config['input_path']}/Transactions.json"
    with open(input_path) as f:
        transactions = json.load(f)
    
    acc_list = client.filter("Accounts")
    contact_list = client.filter("Contacts")

    pushed_ids = []

    for transaction in transactions:
        bank = [acc["AccountID"] for acc in acc_list if acc["Name"]==transaction["Bank"] and acc["Type"]=="BANK"]
        if not bank:
            logger.warning(f"Invalid Bank: {transaction['Bank']}")
        else:
            transaction["BankAccount"] = dict(AccountID=bank[0])
        for line in transaction["LineItems"]:
            code = [acc["Code"] for acc in acc_list if acc["Name"]==line["AccountName"]]
            if not code:
                logger.warning(f"Invalid AccountName: {line['AccountName']}")
            else:
                line["AccountCode"] = code[0]
        contact = [contact["ContactID"] for contact in contact_list if contact["Name"]==transaction["Contact"]]
        if not contact:
            logger.warning(f"Invalid Contact: {transaction['Contact']}")
        else:
            transaction["Contact"] = dict(ContactID=contact[0])
        res = client.push("Bank_Transactions", transaction)
        if res.status_code > 300:
            with open(config["log_file"], "w") as f:
                json.dump(res.json(), f)
            logger.error(f"Invalid Payload: {json.dumps(transaction)}")
            logger.info("Deleting posted transactions")
            for id in pushed_ids:
                client.push("Bank_Transactions", dict(BankTransactionID=id, Status="DELETED"))
            break
        pushed_ids.extend([transaction['BankTransactionID'] for transaction in res['BankTransactions']])

def upload(config, args):
    # Login update tap config with new refresh token if necessary
    try:
        client = XeroClient(config)
        client.refresh_credentials(config, args.config_path)
    except Exception as e:
        with open(config["log_file"], "w") as f:
             json.dump({"Type": "AuthenticationError", "Message": str(e)}, f)
             raise Exception("Authentication Error")

    if os.path.exists(f"{config['input_path']}/Transactions.json"):
        logger.info("Found Transactions.json, uploading...")
        upload_transactions(config, client)
        logger.info("Transactions.json uploaded!")

    if os.path.exists(f"{config['input_path']}/JournalEntries.csv"):
        logger.info("Found JournalEntries.csv, uploading...")
        upload_journals(config, client)
        logger.info("JournalEntries.csv uploaded!")

    logger.info("Posting process has completed!")


def main():
    # Parse command line arguments
    args = parse_args()

    if not args.config.get("log_file"):
        args.config["log_file"] = "error_log.json"

    # Upload the new Xero data
    upload(args.config, args)


if __name__ == "__main__":
    main()
