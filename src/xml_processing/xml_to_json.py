import json 
import xmltodict 

#  python xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set



import argparse
import re

from os import path
import logging
import logging.config



# Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
# Need to set up an S3 bucket to store xml
# Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

# to get set of pmids with search term 'elegans'
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000

pmids = []


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
parser.add_argument('-r', '--restapi', action='store', help='take input from rest api')
parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')
parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')

args = vars(parser.parse_args())

# todo: save this in an env variable
storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_xml/'

# def download_pubmed_xml():
#   for pmid in pmids:
# #    add some validation here
#     url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmid + "&retmode=xml"
#     filename = storage_path + pmid + '.xml'
# #     print url
# #     print filename
#     logger.info("Downloading %s into %s", url, filename)
#     urllib.urlretrieve(url, filename)
#     time.sleep( 5 )


def represents_int(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
  
def month_name_to_number_string(string):
    m = {
        'jan': '01',
        'feb': '02',
        'mar': '03',
        'apr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'aug': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dec': '12'
        }
    s = string.strip()[:3].lower()

    try:
        out = m[s]
        return out
    except:
        raise ValueError(string + ' is not a month')
  
def generate_json():
    # open input xml file and read data in form of python dictionary using xmltodict module 
    for pmid in pmids:
        filename = storage_path + pmid + '.xml'
        with open(filename) as xml_file: 

            xml = xml_file.read()
#             print (xml)
              
            # xmltodict is treating html markup like <i>text</i> as xml, which is creating mistaken structure in the conversion.  
            # may be better to parse full xml instead.
#             data_dict = xmltodict.parse(xml_file.read()) 
            xml_file.close() 
        
            print (pmid)
#             print (data_dict["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]["ArticleTitle"])
        #     if (data_dict["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]["ArticleTitle"])

            data_dict = dict()

            if re.search("<ArticleTitle>(.+?)</ArticleTitle>", xml):
                title_group = re.search("<ArticleTitle>(.+?)</ArticleTitle>", xml)
                title = title_group.group(1)
                print title
                data_dict['title'] = title

            if re.search("<MedlineTA>(.+?)</MedlineTA>", xml):
                journal_group = re.search("<MedlineTA>(.+?)</MedlineTA>", xml)
                journal = journal_group.group(1)
                print journal
                data_dict['journal'] = journal

            if re.search("<MedlinePgn>(.+?)</MedlinePgn>", xml):
                pages_group = re.search("<MedlinePgn>(.+?)</MedlinePgn>", xml)
                pages = pages_group.group(1)
                print pages
                data_dict['pages'] = pages

            if re.search("<Volume>(.+?)</Volume>", xml):
                volume_group = re.search("<Volume>(.+?)</Volume>", xml)
                volume = volume_group.group(1)
                print volume
                data_dict['volume'] = volume

            if re.findall("<PublicationType>(.+?)</PublicationType>", xml):
                types_group = re.findall("<PublicationType>(.+?)</PublicationType>", xml)
                print types_group
                data_dict['pubMedType'] = types_group
            elif re.findall("<PublicationType UI=\".*?\">(.+?)</PublicationType>", xml):
                types_group = re.findall("<PublicationType UI=\".*?\">(.+?)</PublicationType>", xml)
                print types_group
                data_dict['pubMedType'] = types_group

            if re.search("<ArticleId IdType=\"doi\">(.+?)</ArticleId>", xml):
                doi_group = re.search("<ArticleId IdType=\"doi\">(.+?)</ArticleId>", xml)
                doi = doi_group.group(1)
                print doi
                data_dict['doi'] = doi

            # this will need to be restructured to match schema
            if re.findall("<Author.*?>(.+?)</Author>", xml, re.DOTALL):
                authors_group = re.findall("<Author.*?>(.+?)</Author>", xml, re.DOTALL)
                authors_list = []
                for author_xml in authors_group:
                    lastname = ''
                    firstname = ''
                    firstinit = ''
                    if re.search("<LastName>(.+?)</LastName>", author_xml):
                        lastname_group = re.search("<LastName>(.+?)</LastName>", author_xml)
                        lastname = lastname_group.group(1)
                    if re.search("<ForeName>(.+?)</ForeName>", author_xml):
                        firstname_group = re.search("<ForeName>(.+?)</ForeName>", author_xml)
                        firstname = firstname_group.group(1)
                    if re.search("<Initials>(.+?)</Initials>", author_xml):
                        firstinit_group = re.search("<Initials>(.+?)</Initials>", author_xml)
                        firstinit = firstinit_group.group(1)
                    fullname = firstname + ' ' + lastname
                    author_dict = {}
                    author_dict["firstname"] = firstname
                    author_dict["firstinit"] = firstinit
                    author_dict["lastname"] = lastname
                    author_dict["fullname"] = fullname
                    print fullname
                    authors_list.append(author_dict)
                data_dict['authors'] = authors_list

            if re.search("<PubDate>(.+?)</PubDate>", xml, re.DOTALL):
                pub_date_group = re.search("<PubDate>(.+?)</PubDate>", xml, re.DOTALL)
                pub_date = pub_date_group.group(1)
#                 print pub_date
                year = '';
                month = '';
                day = '';
                date_list = []
                if re.search("<Year>(.+?)</Year>", pub_date, re.DOTALL):
                    year_group = re.search("<Year>(.+?)</Year>", pub_date, re.DOTALL)
                    year = year_group.group(1)
                    date_list.append(year)
                if re.search("<Month>(.+?)</Month>", pub_date):
                    month_group = re.search("<Month>(.+?)</Month>", pub_date)
                    month_text = month_group.group(1)
                    if represents_int(month_text):
                        month = month_text
                    else:
                        month = month_name_to_number_string(month_text)
                    date_list.append(month)
                if re.search("<Day>(.+?)</Day>", pub_date):
                    day_group = re.search("<Day>(.+?)</Day>", pub_date)
                    day = day_group.group(1)
                    date_list.append(day)
                date_string = "-".join(date_list)
                print date_string
                date_dict = {}
                date_dict['date_string'] = date_string
                date_dict['year'] = year
                date_dict['month'] = month
                date_dict['day'] = day
                data_dict['datePublished'] = date_dict


#   my ($journal) = $page =~ /<MedlineTA>(.+?)\<\/MedlineTA\>/i;
#   my ($pages) = $page =~ /\<MedlinePgn\>(.+?)\<\/MedlinePgn\>/i;
#   my ($volume) = $page =~ /\<Volume\>(.+?)\<\/Volume\>/i;
#   my ($title) = $page =~ /\<ArticleTitle\>(.+?)\<\/ArticleTitle\>/i;
#   my (@types) = $page =~ /\<PublicationType\>(.+?)\<\/PublicationType\>/gi;
#   unless ($types[0]) {
#     (@types) = $page =~ /\<PublicationType UI=\".*?\"\>(.+?)\<\/PublicationType\>/gi; }
#   my ($doi) = $page =~ /\<ArticleId IdType=\"doi\"\>(.+?)\<\/ArticleId\>/i; if ($doi) { $doi = 'doi' . $doi; }
#   my @xml_authors = $page =~ /\<Author.*?\>(.+?)\<\/Author\>/ig;
#   my @authors;
#   foreach (@xml_authors){
#       my ($lastname, $initials) = $_ =~ /\<LastName\>(.+?)\<\/LastName\>.+\<Initials\>(.+?)\<\/Initials\>/i;
#       my $author = $lastname . " " . $initials; push @authors, $author; }
#   if ( $page =~ /\<PubDate\>(.+?)\<\/PubDate\>/si ) {
#     my ($PubDate) = $page =~ /\<PubDate\>(.+?)\<\/PubDate\>/si;
#     if ( $PubDate =~ /\<Year\>(.+?)\<\/Year\>/i ) { $year = $1; }
#     if ( $PubDate =~ /\<Month\>(.+?)\<\/Month\>/i ) { $month = $1;
#       if ($month_to_num{$month}) { $month = $month_to_num{$month}; }
#       else {          # in one case 00013115 / pmid12167287, it says Jul-Sep
#         foreach my $key (keys %month_to_num) {        # so see if it begins with any month and use that
#           if ($month =~ m/^$key/) { $month = $month_to_num{$key}; } } } }
#     if ( $PubDate =~ /\<Day\>(.+?)\<\/Day\>/i ) { $day = $1; if ($day =~ m/^0/) { $day =~ s/^0//; } } }
            
            # generate the object using json.dumps()  
            # corresponding to json data 
              
            # minified
            # json_data = json.dumps(data_dict) 
        
            # pretty-print
            json_data = json.dumps(data_dict, indent=4, sort_keys=True) 
        
            # Write the json data to output json file 
            json_storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_json/'
            json_filename = json_storage_path + pmid + '.json'
            with open(json_filename, "w") as json_file: 
                json_file.write(json_data) 
                json_file.close() 


if __name__ == "__main__":
    """ call main start function """

#    python xml_to_json.py -d
    if args['database']:
        logger.info("Processing database entries")

    elif args['restapi']:
        logger.info("Processing rest api entries")

#     python xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

#     python xml_to_json.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args['url']:
        logger.info("Processing url input from %s", args['url'])
        req = urllib.urlopen(args['url'])
        data = req.read()
        lines = data.splitlines()
        for pmid in lines:
            pmids.append(pmid)

#    python xml_to_json.py -c 1234 4576 1828
    elif args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids.append(pmid)

#    python xml_to_json.py -s
    elif args['sample']:
        logger.info("Processing hardcoded sample input")
        pmid = '12345678'
        pmids.append(pmid)
        pmid = '12345679'
        pmids.append(pmid)
        pmid = '12345680'
        pmids.append(pmid)

    else:
        logger.info("Processing database entries")

#     download_pubmed_xml()
    generate_json()



#   my %month_to_num;
#   $month_to_num{Jan} = '1';
#   $month_to_num{Feb} = '2';
#   $month_to_num{Mar} = '3';
#   $month_to_num{Apr} = '4';
#   $month_to_num{May} = '5';
#   $month_to_num{Jun} = '6';
#   $month_to_num{Jul} = '7';
#   $month_to_num{Aug} = '8';
#   $month_to_num{Sep} = '9';
#   $month_to_num{Oct} = '10';
#   $month_to_num{Nov} = '11';
#   $month_to_num{Dec} = '12';
# 
#   my ($title) = $page =~ /\<ArticleTitle\>(.+?)\<\/ArticleTitle\>/i;
#   my ($journal) = $page =~ /<MedlineTA>(.+?)\<\/MedlineTA\>/i;
#   my ($pages) = $page =~ /\<MedlinePgn\>(.+?)\<\/MedlinePgn\>/i;
#   my ($volume) = $page =~ /\<Volume\>(.+?)\<\/Volume\>/i;
#   my $year = ''; my $month = ''; my $day = '';
#   if ( $page =~ /\<PubDate\>(.+?)\<\/PubDate\>/si ) {
#     my ($PubDate) = $page =~ /\<PubDate\>(.+?)\<\/PubDate\>/si;
#     if ( $PubDate =~ /\<Year\>(.+?)\<\/Year\>/i ) { $year = $1; }
#     if ( $PubDate =~ /\<Month\>(.+?)\<\/Month\>/i ) { $month = $1;
#       if ($month_to_num{$month}) { $month = $month_to_num{$month}; }
#       else {          # in one case 00013115 / pmid12167287, it says Jul-Sep
#         foreach my $key (keys %month_to_num) {        # so see if it begins with any month and use that
#           if ($month =~ m/^$key/) { $month = $month_to_num{$key}; } } } }
#     if ( $PubDate =~ /\<Day\>(.+?)\<\/Day\>/i ) { $day = $1; if ($day =~ m/^0/) { $day =~ s/^0//; } } }
#   my (@types) = $page =~ /\<PublicationType\>(.+?)\<\/PublicationType\>/gi;
#   unless ($types[0]) {
#     (@types) = $page =~ /\<PublicationType UI=\".*?\"\>(.+?)\<\/PublicationType\>/gi; }
#   my ($abstract) = $page =~ /\<AbstractText\>(.+?)\<\/AbstractText\>/i;
#   unless ($abstract) {                          # if there is no abstract match, try to get label and concatenate multiple matches.
#     my @abstracts = $page =~ /\<AbstractText(.+?)\<\/AbstractText\>/gi;
#     foreach my $ab (@abstracts) {
#       if ($ab =~ m/Label=\"(.*?)\"/i) { $abstract .= "${1}: "; }
#       if ($ab =~ m/^.*\>/) { $ab =~ s/^.*\>//; } $abstract .= "$ab "; }
#     if ($abstract =~ m/ +$/) { $abstract =~ s/ +$//; } }
#   my ($doi) = $page =~ /\<ArticleId IdType=\"doi\"\>(.+?)\<\/ArticleId\>/i; if ($doi) { $doi = 'doi' . $doi; }
#   my $pubmed_final = 'not_final';
#   my $medline_citation = '';
#   if ($page =~ m/(\<MedlineCitation.*?>)/) { $medline_citation = $1; }
#   if ($medline_citation =~ /\<MedlineCitation .*Status=\"MEDLINE\"\>/i) { $pubmed_final = 'final'; }    # final version
#   elsif ($medline_citation =~ /\<MedlineCitation .*Status=\"PubMed-not-MEDLINE\"\>/i) { $pubmed_final = 'final'; }      # final version
#   elsif ($medline_citation =~ /\<MedlineCitation .*Status=\"OLDMEDLINE\"\>/i) { $pubmed_final = 'final'; }      # final version
# 
#   my @xml_authors = $page =~ /\<Author.*?\>(.+?)\<\/Author\>/ig;
#   my @authors;
#   foreach (@xml_authors){
#       my ($lastname, $initials) = $_ =~ /\<LastName\>(.+?)\<\/LastName\>.+\<Initials\>(.+?)\<\/Initials\>/i;
#       my $author = $lastname . " " . $initials; push @authors, $author; }

