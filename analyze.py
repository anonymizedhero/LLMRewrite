import csv

# qid,round,query,rewrite_r1,rewrite_r2,rewrite,total_cost,suggest_cost,semantic_cost,syntax_cost,semantic_iterations,syntax_iterations,query_explain_cost,rewrite_explain_cost
input_file = './exp_results/tpcds_gpt-4o_0707-1842.csv'
with open(input_file, 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        qid = row[0]
        round = row[1]
        query = row[2]
        rewrite = row[5]
        query_explain_cost = row[12]
        rewrite_explain_cost = row[13]

        # if qid in ['12', '20', '22', '23', '88']:
        if qid in ['23']:
            # print(f'qid={qid}, round={round}, query={query}, rewrite={rewrite}, query_explain_cost={query_explain_cost}, rewrite_explain_cost={rewrite_explain_cost}')
            print(f'qid={qid}, round={round}, query_explain_cost={query_explain_cost}, rewrite_explain_cost={rewrite_explain_cost}, ratio={float(rewrite_explain_cost)/float(query_explain_cost)}')
            print(f'rewrite={rewrite}')