import argparse, psycopg, os, sys, pathlib

# make sure db config is set in environment variables
required_env_vars = ['GEODE_DB_USER', 'GEODE_DB_PASSWORD', 'GEODE_DB_HOST']
missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_env_vars:
    print(f"[ERROR] Missing required environment variables: {', '.join(missing_env_vars)}", file=sys.stderr)
    sys.exit(1)

# make argument parser
parser = argparse.ArgumentParser(
    prog='nf-core-treeinference fasta generator',
    formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(prog, width=80),
    description='Generates fasta files for the nf-core-treeinference pipeline from the GEODE database.',
    epilog="The --taxonomy and --exclude-taxonomy arguments should be .csv files with the header 'family,genus,species,rapid_id'. All columns are optional, and each row is evaluated as specifically as possible. For example, ',Ischnura,elegans,' will match all samples of the species Ischnura elegans. ',,,105603_P001_WA01' will match only the unique RAPID id for 20kb, plate 1, well A01. 'Coenagrionidae,,,' will match all samples within the family Coenagrionidae. See the example taxonomy.csv file for a reference.")

# custom type for --taxonomy argument to allow either a file or 'all'
def taxonomy_file(path_string):
    if path_string.lower() == 'all':
        return 'all'
    path = pathlib.Path(path_string)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"Taxonomy file '{path}' does not exist")
    return path

# add arguments
parser.add_argument('--taxonomy',
    required=True,
    type=taxonomy_file,
    metavar='FILE',
    help="A .csv file with taxonomy to include. See below for file instructions. Alternatively, specify 'all' to use all samples which pass the minimum values.")
parser.add_argument('--taxonomy-exclude',
    metavar='FILE',
    help="A .csv file with taxonomy to exclude. See below for file instructions.")
parser.add_argument('--min-avg-kmer-coverage',
    default=50,
    type=int,
    metavar='INT',
    help="Minimum value for the average SPAdes kmer coverage per sample.")
parser.add_argument('--min-locus-kmer-coverage',
    default=50,
    type=int,
    metavar='INT',
    help="Minimum value for the SPAdes kmer coverage per locus.")
parser.add_argument('--min-num-loci',
    default=0,
    type=int,
    metavar='INT',
    help="Minimum value for the number of recovered loci per sample.")
parser.add_argument('--include-outgroups',
    default=False,
    action='store_true',
    help="Specify this flag to include Ephemeroptera outgroups.")

args = parser.parse_args()

# parse taxonomy files
#with open(args.taxonomy) as f:

# make db connection

conn_string = f"dbname=geode user={os.environ['GEODE_DB_USER']} password={os.environ['GEODE_DB_PASSWORD']} host={os.environ['GEODE_DB_HOST']}"
with psycopg.connect(conn_string, row_factory=psycopg.rows.dict_row) as conn:


    query = 'SELECT id FROM v_sequence_qc WHERE assembly_kmer_coverage > %s'
    result = conn.execute(query, (args.min_avg_kmer_coverage,)).fetchall()
    selected_sample_ids = [x['id'] for x in result]

    # first get samples that pass by average kmer coverage
    # then get sequences and filter them for kmer coverage
    # finally filter samples to make sure they have the min num of loci
    # order by locus to stream directly to file
    query = '''
    SELECT
        r.name AS sample,
        l.name AS locus,
        ls.locus_sequence AS locus_sequence
    FROM
        locus_sequence ls
        LEFT JOIN rapid r ON ls.sample_id = r.id
        LEFT JOIN locus l ON ls.locus_id = l.id
    WHERE ls.sample_id IN (
        SELECT sample_id FROM (
            SELECT
                sample_id,
                count(sample_id) AS locus_count
            FROM locus_sequence
            WHERE
                sample_id IN (
                    SELECT id FROM v_sequence_qc
                    WHERE assembly_kmer_coverage >= %(assembly_kmer_coverage)s
                )
                AND kmer_coverage >= %(kmer_coverage)s
                GROUP BY sample_id
        )
        WHERE locus_count >= %(locus_count)s
    )
    ORDER BY locus ASC
    '''
    result = conn.execute(query, {'assembly_kmer_coverage': args.min_avg_kmer_coverage, 'kmer_coverage': args.min_locus_kmer_coverage, 'locus_count': args.min_num_loci}).fetchall()

    # stream to locus files
    pathlib.Path('output').mkdir(exist_ok=True)
    current_locus = None
    current_file = None

    for row in result:
        if row['locus'] != current_locus:
            if current_file: current_file.close()
            current_locus = row['locus']
            current_file = open(f"output/{current_locus}.fasta", 'w')
        current_file.write(f">{row['sample']}\n{row['locus_sequence']}\n")

    if current_file: current_file.close()
