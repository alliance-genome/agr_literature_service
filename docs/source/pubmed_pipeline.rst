PubMed XML Pipeline
===================


XML Processing
--------------

Configuration
^^^^^^^^^^^^

If running manually

- replace ``.env XML_PATH`` with the path to where processing will happen,
- leave ``API_PORT 8080`` if running from within docker but change to API port if outside docker
- set ``OKTA_CLIENT_ID`` and ``OKTA_CLIENT_SECRET``
- If stored in ``.bashrc`` or ``.zshrc`` just remove those four variables from ``.env`` file and ``pipenv`` will get them from the shell ``rc``.


Scripts
^^^^^^^

Below is a list of the scripts and the order to run them

get dqm data from fms
^^^^^^^^^^^^^^^^^^^^^^

To get dqm data::

    python3 src/xml_processing/get_dqm_data.py

This scripts accepts as inputs::


    mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
    datatypes = ['REFERENCE', 'REF-EXCHANGE', 'RESOURCE']
    'https://fms.alliancegenome.org/api/datafile/by/' + release + '/' + datatype + '/' + mod + '?latest=true'

and the expected output are files on

    ``dqm_data/<datatype>_<mod>.json``

