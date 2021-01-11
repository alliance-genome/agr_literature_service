
import time
import urllib
import argparse


# Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
# Need to set up an S3 bucket to store xml
# Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

# to get set of pmids with search term 'elegans'
# my $url = "https:\/\/eutils\.ncbi\.nlm\.nih\.gov\/entrez\/eutils\/esearch\.fcgi\?db\=pubmed\&term\=elegans\&retmax=100000000";

pmids = []

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')
parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')

args = vars(parser.parse_args())

# todo: save this in an env variable
storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/pubmed_xml/'

def download_pubmed_xml():
  for pmid in pmids:
#    add some validation here
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmid + "&retmode=xml"
    print url
    filename = storage_path + pmid + '.xml'
    urllib.urlretrieve(url, filename)
    time.sleep( 5 )



if __name__ == "__main__":
    """ call main start function """

#    python get_pubmed_xml.py -d
    if args['database']:
        print "database"

#     python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/inputs/pmid_file.txt
    elif args['file']:
        print "file"
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            pmids.append(pmid)

#     python get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args['url']:
        print "url"
        req = urllib.urlopen(args['url'])
        data = req.read()
        lines = data.splitlines()
#         lines = req.readlines()
        for pmid in lines:
            pmids.append(pmid)

#    python get_pubmed_xml.py -c 1234 4576 1828
    elif args['commandline']:
        print "commandline"
        for pmid in args['commandline']:
            pmids.append(pmid)

#    python get_pubmed_xml.py -s
    elif args['sample']:
        print "sample"
        pmid = '12345678'
        pmids.append(pmid)
        pmid = '12345679'
        pmids.append(pmid)
        pmid = '12345680'
        pmids.append(pmid)

    else:
        print "database"

    download_pubmed_xml()

