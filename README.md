# AGR Literature Service

## Overview
The Alliance of Genome Resources (AGR) Literature Service is the backend component of the Alliance Bibliography Central
(ABC) platform. It provides a number of reference management functions, including:

- **Database interface**: Manages the central ABC database and provides interfaces to CRUD operations through RESTful API endpoints
- **Search**: Creates an elasticsearch index from the database, keeps the data in sync and provides access to the indexed reference data
- **Literature processing scripts**: Fetches references and other related data from PubMed and the MODs
- **File management**: Provides a simplified interface to reference files management
- **Automated reports**: Generates automated reports for curators
- **Automated scripts**: Runs scripts periodically

Please refer to the Alliance Confluence page for more information and documentation on the project.