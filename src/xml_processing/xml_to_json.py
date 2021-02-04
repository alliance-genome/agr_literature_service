import json 
import xmltodict 

#  python xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set

# update sample
# cp pubmed_json/32542232.json pubmed_sample
# cp pubmed_json/32644453.json pubmed_sample
# cp pubmed_json/33408224.json pubmed_sample
# cp pubmed_json/33002525.json pubmed_sample
# cp pubmed_json/33440160.json pubmed_sample
# cp pubmed_json/33410237.json pubmed_sample
# git add pubmed_sample/32542232.json 
# git add pubmed_sample/32644453.json 
# git add pubmed_sample/33408224.json 
# git add pubmed_sample/33002525.json 
# git add pubmed_sample/33440160.json 
# git add pubmed_sample/33410237.json 


# https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt


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


known_article_id_types = { 'pubmed', 'doi', 'pmc', 'pii' }
unknown_article_id_types = set()
  

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

def get_year_month_day_from_xml_date(pub_date):
    date_list = []
    year = '';
    month = '01';
    day = '01';
    year_re_output = re.search("<Year>(.+?)</Year>", pub_date)
    if year_re_output is not None:
        year = year_re_output.group(1)
    month_re_output = re.search("<Month>(.+?)</Month>", pub_date)
    if month_re_output is not None:
        month_text = month_re_output.group(1)
        if represents_int(month_text):
            month = month_text
        else:
            month = month_name_to_number_string(month_text)
    day_re_output = re.search("<Day>(.+?)</Day>", pub_date)
    if day_re_output is not None:
        day = day_re_output.group(1)
    date_list.append(year)
    date_list.append(month)
    date_list.append(day)
    return date_list


  
def generate_json():
    # open input xml file and read data in form of python dictionary using xmltodict module 
    for pmid in pmids:
        filename = storage_path + pmid + '.xml'
        if not path.exists(filename):
            continue
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

            title_re_output = re.search("<ArticleTitle>(.+?)</ArticleTitle>", xml)
            if title_re_output is not None:
#                 print title
                data_dict['title'] = title_re_output.group(1)

            journal_re_output = re.search("<MedlineTA>(.+?)</MedlineTA>", xml)
            if journal_re_output is not None:
#                 print journal
                data_dict['journal'] = journal_re_output.group(1) 

            pages_re_output = re.search("<MedlinePgn>(.+?)</MedlinePgn>", xml)
            if pages_re_output is not None:
#                 print pages
                data_dict['pages'] = pages_re_output.group(1)

            volume_re_output = re.search("<Volume>(.+?)</Volume>", xml)
            if volume_re_output is not None:
#                 print volume
                data_dict['volume'] = volume_re_output.group(1)

            issue_re_output = re.search("<Issue>(.+?)</Issue>", xml)
            if issue_re_output is not None:
#                 print issue
                data_dict['issueName'] = issue_re_output.group(1)

            if re.findall("<PublicationType>(.+?)</PublicationType>", xml):
                types_group = re.findall("<PublicationType>(.+?)</PublicationType>", xml)
#                 print types_group
                data_dict['pubMedType'] = types_group
            elif re.findall("<PublicationType UI=\".*?\">(.+?)</PublicationType>", xml):
                types_group = re.findall("<PublicationType UI=\".*?\">(.+?)</PublicationType>", xml)
#                 print types_group
                data_dict['pubMedType'] = types_group

# Do these all together later
#             if re.search("<ArticleId IdType=\"doi\">(.+?)</ArticleId>", xml):
#                 doi_group = re.search("<ArticleId IdType=\"doi\">(.+?)</ArticleId>", xml)
#                 doi = doi_group.group(1)
# #                 print doi
#                 data_dict['doi'] = doi

            # this will need to be restructured to match schema
            authors_group = re.findall("<Author.*?>(.+?)</Author>", xml, re.DOTALL)
            if len(authors_group) > 0:
                authors_list = []
                authors_rank = 0
                for author_xml in authors_group:
                    authors_rank = authors_rank + 1
                    lastname = ''
                    firstname = ''
                    firstinit = ''
                    lastname_re_output = re.search("<LastName>(.+?)</LastName>", author_xml)
                    if lastname_re_output is not None:
                        lastname = lastname_re_output.group(1)
                    firstname_re_output = re.search("<ForeName>(.+?)</ForeName>", author_xml)
                    if firstname_re_output is not None:
                        firstname = firstname_re_output.group(1)
                    firstinit_re_output = re.search("<Initials>(.+?)</Initials>", author_xml)
                    if firstinit_re_output is not None:
                        firstinit = firstinit_re_output.group(1)
                    if firstinit and not firstname:
                        firstname = firstinit
                    fullname = firstname + ' ' + lastname
                    author_dict = {}
#                     if (firstname and firstinit):
#                         print "GOOD\t" + pmid
#                     elif firstname:
#                         print "FN\t" + pmid + "\t" + firstname
#                     elif firstinit:
#                         print "FI\t" + pmid + "\t" + firstinit
#                     else:
#                         print "NO\t" + pmid
                    author_dict["firstname"] = firstname
                    author_dict["firstinit"] = firstinit
                    author_dict["lastname"] = lastname
                    author_dict["name"] = fullname
                    author_dict["authorRank"] = authors_rank
#                     print fullname
                    authors_list.append(author_dict)
                data_dict['authors'] = authors_list

# parse ORCID
#                     <Identifier Source="ORCID">0000-0002-0184-8324</Identifier>


            pub_date_re_output = re.search("<PubDate>(.+?)</PubDate>", xml, re.DOTALL)
            if pub_date_re_output is not None:
                pub_date = pub_date_re_output.group(1)
                date_list = get_year_month_day_from_xml_date(pub_date)
                if date_list[0]:
                    date_string = "-".join(date_list)
#                     print date_string
                    date_dict = {}
                    date_dict['date_string'] = date_string
                    date_dict['year'] = date_list[0]
                    date_dict['month'] = date_list[1]
                    date_dict['day'] = date_list[2]
                    data_dict['datePublished'] = date_dict
                    data_dict['issueDate'] = date_dict

            date_revised_re_output = re.search("<DateRevised>(.+?)</DateRevised>", xml, re.DOTALL)
            if date_revised_re_output is not None:
                date_revised = date_revised_re_output.group(1)
                date_list = get_year_month_day_from_xml_date(date_revised)
                if date_list[0]:
                    date_string = "-".join(date_list)
#                     print date_string
                    date_dict = {}
                    date_dict['date_string'] = date_string
                    date_dict['year'] = date_list[0]
                    date_dict['month'] = date_list[1]
                    date_dict['day'] = date_list[2]
                    data_dict['dateLastModified'] = date_dict

            date_received_re_output = re.search("<PubMedPubDate PubStatus=\"received\">(.+?)</PubMedPubDate>", xml, re.DOTALL)
            if date_received_re_output is not None:
                date_received = date_received_re_output.group(1)
                date_list = get_year_month_day_from_xml_date(date_received)
                if date_list[0]:
                    date_string = "-".join(date_list)
#                     print date_string
                    date_dict = {}
                    date_dict['date_string'] = date_string
                    date_dict['year'] = date_list[0]
                    date_dict['month'] = date_list[1]
                    date_dict['day'] = date_list[2]
                    data_dict['dateArrivedInPubmed'] = date_dict

            article_id_list_re_output = re.search("<ArticleIdList>(.*?)</ArticleIdList>", xml, re.DOTALL)
            if article_id_list_re_output is not None:
                article_id_list = article_id_list_re_output.group(1)
#                 print pmid + " AIDL " + article_id_list
                article_id_group = re.findall("<ArticleId IdType=\"(.*?)\">(.+?)</ArticleId>", article_id_list, re.DOTALL)
                if len(article_id_group) > 0:
                    for type_value in article_id_group:
                        type = type_value[0]
                        value = type_value[1]
#                         print pmid + " type " + type + " value " + value
                        if type not in known_article_id_types:
                            unknown_article_id_types.add(type)
                        if type in data_dict:
                            print pmid + " has multiple for type " + type
                        data_dict[type] = value

            medline_journal_info_re_output = re.search("<MedlineJournalInfo>(.*?)</MedlineJournalInfo>", xml, re.DOTALL)
            if medline_journal_info_re_output is not None:
                medline_journal_info = medline_journal_info_re_output.group(1)
#                 print pmid + " medline_journal_info " + medline_journal_info
                nlm = '';
                issn = '';
                journal_abbrev = '';
                nlm_re_output = re.search("<NlmUniqueID>(.+?)</NlmUniqueID>", medline_journal_info)
                if nlm_re_output is not None:
                    nlm = nlm_re_output.group(1)
                issn_re_output = re.search("<ISSNLinking>(.+?)</ISSNLinking>", medline_journal_info)
                if issn_re_output is not None:
                    issn = issn_re_output.group(1)
                journal_abbrev_re_output = re.search("<MedlineTA>(.+?)</MedlineTA>", medline_journal_info)
                if journal_abbrev_re_output is not None:
                    journal_abbrev = journal_abbrev_re_output.group(1)
                data_dict['nlm'] = nlm
                data_dict['issn'] = issn
                data_dict['resourceAbbreviation'] = journal_abbrev
#                 check whether all xml has an nlm or issn, for WB set, they all do
#                 if (nlm and issn):
#                     print "GOOD\t" + pmid
#                 elif nlm:
#                     print "NLM\t" + pmid + "\t" + nlm
#                 elif issn:
#                     print "ISSN\t" + pmid + "\t" + issn
#                 else:
#                     print "NO\t" + pmid

            publisher_re_output = re.search("<PublisherName>(.+?)</PublisherName>", xml)
            if publisher_re_output is not None:
                publisher = publisher_re_output.group(1)
#                 print publisher
                data_dict['publisher'] = publisher

            regex_keyword_output = re.findall("<Keyword .*?>(.+?)</Keyword>", xml, re.DOTALL)
            if len(regex_keyword_output) > 0:
                data_dict['keywords'] = regex_keyword_output

            regex_abstract_output = re.findall("<AbstractText.*?>(.+?)</AbstractText>", xml, re.DOTALL)
            if len(regex_abstract_output) > 0:
                data_dict['abstract'] = " ".join(regex_abstract_output)

            regex_keyword_output = re.findall("<Keyword .*?>(.+?)</Keyword>", xml, re.DOTALL)
            if len(regex_keyword_output) > 0:
                data_dict['keywords'] = regex_keyword_output

            meshs_group = re.findall("<MeshHeading>(.+?)</MeshHeading>", xml, re.DOTALL)
            if len(meshs_group) > 0:
                meshs_list = []
                for mesh_xml in meshs_group:
                    descriptor_re_output = re.search("<DescriptorName.*?>(.+?)</DescriptorName>", mesh_xml, re.DOTALL)
                    if descriptor_re_output is not None:
                        mesh_heading_term = descriptor_re_output.group(1)
                        qualifier_group = re.findall("<QualifierName.*?>(.+?)</QualifierName>", mesh_xml, re.DOTALL)
                        if len(qualifier_group) > 0:
                            for mesh_qualifier_term in qualifier_group:
                                mesh_dict = {}
                                mesh_dict["referenceId"] = pmid
                                mesh_dict["meshHeadingTerm"] = mesh_heading_term
                                mesh_dict["meshQualfierTerm"] = mesh_qualifier_term
                                meshs_list.append(mesh_dict)
                        else:
                            mesh_dict = {}
                            mesh_dict["referenceId"] = pmid
                            mesh_dict["meshHeadingTerm"] = mesh_heading_term
                            meshs_list.append(mesh_dict)
#                 for mesh_xml in meshs_group:
#                     descriptor_group = re.findall("<DescriptorName.*?UI=\"(.+?)\".*?>(.+?)</DescriptorName>", mesh_xml, re.DOTALL)
#                     if len(descriptor_group) > 0:
#                         for id_name in descriptor_group:
#                             mesh_dict = {}
#                             mesh_dict["referenceId"] = id_name[0]
#                             mesh_dict["meshHeadingTerm"] = id_name[1]
#                             meshs_list.append(mesh_dict)
#                     qualifier_group = re.findall("<QualifierName.*?UI=\"(.+?)\".*?>(.+?)</QualifierName>", mesh_xml, re.DOTALL)
#                     if len(qualifier_group) > 0:
#                         for id_name in qualifier_group:
#                             mesh_dict = {}
#                             mesh_dict["referenceId"] = id_name[0]
#                             mesh_dict["meshQualfierTerm"] = id_name[1]
#                             meshs_list.append(mesh_dict)
                data_dict['meshTerms'] = meshs_list

            
            # generate the object using json.dumps()  
            # corresponding to json data 
              
            # minified
            # json_data = json.dumps(data_dict) 
        
            # pretty-print
            json_data = json.dumps(data_dict, indent=4, sort_keys=True) 

            # Write the json data to output json file 
# UNCOMMENT TO write to json directory
            json_storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_json/'
            json_filename = json_storage_path + pmid + '.json'
            with open(json_filename, "w") as json_file: 
                json_file.write(json_data) 
                json_file.close() 

    for unknown_article_id_type in unknown_article_id_types:
        logger.info("unknown_article_id_type %s", unknown_article_id_type)
        


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


# capture ISSN / NLM
#         <MedlineJournalInfo>
#             <Country>England</Country>
#             <MedlineTA>J Travel Med</MedlineTA>
#             <NlmUniqueID>9434456</NlmUniqueID>
#             <ISSNLinking>1195-1982</ISSNLinking>
#         </MedlineJournalInfo>
# not from
#             <Journal>
#                 <ISSN IssnType="Electronic">1708-8305</ISSN>


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

