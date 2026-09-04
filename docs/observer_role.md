# Cognito Observer role (SCRUM-6431 / SCRUM-6429)

An **observer** is a MOD-sponsored collaborator with read-only access to the
ABC: they can sign in, search, view reference data, and download open-access
plus their sponsoring MOD's restricted full text and derived files — but cannot
create, update, or delete anything.

## Group convention

One Cognito group per MOD, resolved through the explicit mapping in
`agr_literature_service/api/observer.py` (`OBSERVER_GROUP_TO_MOD`) — never by
string-prefix inference. Adding a MOD is a one-line mapping change; all
observer groups share the same capability policy.

| Cognito group | Sponsoring MOD |
|---|---|
| SGDObserver | SGD |
| RGDObserver | RGD |
| MGIObserver | MGI |
| ZFINObserver | ZFIN |
| XenbaseObserver | XB |
| FlyBaseObserver | FB |
| WormBaseObserver | WB |

Role precedence: any write-capable role (a `*Curator` group, `SuperAdmin` /
`AdminGroup` / `*Developer`, or a service-account access token) supersedes
observer membership — granting an observer group to an existing curator
changes nothing, and observer membership never triggers curator automations.

## Capability policy

Content **visibility** and **mutation capability** are deliberately separate:

* `agr_cognito_py.get_mod_access` (which gates deletions and other mutations)
  still returns `NO_ACCESS` for observers.
* `observer.visibility_mod_access` returns the sponsoring MOD's access value
  and is used **only** by the restricted-file download paths
  (`/reference/referencefile/download_file`, `/additional_files_tarball`), so a
  FB observer can download FB-supplied restricted full text but not another
  MOD's.

## Server-side read-only enforcement

`enforce_observer_read_only` runs inside the shared authentication dependency
(`IPAwareCognitoAuth`) for every authenticated request (bearer token and
session cookie alike), so the API rejects observer mutations with **403**
regardless of what any UI exposes:

* `GET` / `HEAD` / `OPTIONS`: always allowed.
* `POST` / `PUT` / `PATCH` / `DELETE`: rejected, **except** the read-only POST
  endpoints below (data retrieval / session management that happen to use POST
  bodies):

```
POST /auth/login
POST /auth/logout
POST /search/references
POST /ontology/term_details
POST /cross_reference/show_all
POST /topic_entity_tag/by_references
POST /reference/referencefile/show_main_pdf_ids_for_curies
POST /xml2md/convert
POST /xml2md/validate
```

### User-session access tokens are rejected

`agr_cognito_py` treats every `token_use=access` token as an ALL_ACCESS service
account and discards its real `cognito:groups`, so a browser session's access
token would sidestep every role decision — including this read-only boundary.
The auth layer therefore rejects access tokens carrying the
`aws.cognito.signin.user.admin` scope (present on every user-pool sign-in
token, never on client-credentials tokens) with 401. Optionally set
`COGNITO_SERVICE_CLIENT_IDS` (comma-separated) to additionally restrict service
tokens to an explicit client_id allowlist. The UI authenticates with the ID
token and is unaffected.

Known side door: `POST /reference/referencefile/bulk_upload_validate/`
authenticates via `get_cognito_user_swagger` directly rather than the shared
dependency, so it skips both this gate and the observer check; it is read-only
(a validator), so this is accepted — anything added to it beyond validation
must first move it onto `get_authenticated_user`.

Known limitation (v1): `POST /person_setting/` is not allowlisted, so
observers cannot persist saved searches/preferences; revisit if the WG wants
observer preferences.

## First-login auto-registration

On the first ID-token request from an email the ABC does not know,
`set_global_user_from_cognito` registers the user: if an admin pre-registered
a person with that email, the login links a person-backed `users` row to that
person (setting `mod_roles` from the Cognito groups only when unset);
otherwise it creates the person (with `person.mod_roles` recording the Cognito
groups), the email, and the `users` row in one transaction. Concurrent
first-login requests are serialized with a per-email advisory lock, so the
UI's parallel request burst cannot create duplicate persons. Only accounts
carrying a recognized role group (curator / admin / developer / observer) are
registered; anything else — and any registration failure — gets the
contact-an-administrator 403.

## GET endpoint inventory

The 164 GET endpoints below are what an observer can reach (subject to the
per-endpoint data rules above). Generated from the FastAPI routers; regenerate
by dumping `app.routes`.

```
/auth/status
/author/{author_id}
/author/{author_id}/versions
/bulk_download/references/external_ids/
/bulk_download/resources/external_ids/
/check/ateamapi
/check/check_duplicate_orcids
/check/check_obsolete_entities
/check/check_obsolete_pmids
/check/check_redacted_references_with_tags
/check/database
/check/debezium_status
/check/environments
/check/qc_report_dates/{report_key}
/copyright_license/all
/cross_reference/check/curie/{datatype}/{curie}
/cross_reference/check/patterns/{datatype}
/cross_reference/{cross_reference_id}/versions
/cross_reference/{curie:path}
/curation_status/aggregated_curation_status_and_tet_info/{reference_curie}/{mod_abbreviation}
/curation_status/{curation_status_id}
/database/configuration
/database/schema/download
/datasets/download/{mod_abbreviation}/{data_type}/{dataset_type}/
/datasets/download/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/
/datasets/metadata/{mod_abbreviation}/{data_type}/{dataset_type}/
/datasets/metadata/{mod_abbreviation}/{data_type}/{dataset_type}/{version}/
/docs
/docs/oauth2-redirect
/editor/{editor_id}
/editor/{editor_id}/versions
/image_permission/all
/image_permission/resource/all
/image_permission/resource/{resource_curie}
/image_permission/resource_link/{resource_image_permission_id}
/image_permission/{image_permission_id}
/indexing_priority/get_priority_tag/{reference_curie}/{mod_abbreviation}
/indexing_priority/{indexing_priority_id}
/laboratory/by_laboratory_cross_reference/{curie_or_laboratory_cross_reference_id}
/laboratory/by_name
/laboratory/by_strain_designation
/laboratory/{curie_or_laboratory_id}
/laboratory_allele_designation/laboratory/{curie_or_laboratory_id}
/laboratory_allele_designation/{laboratory_allele_designation_id}
/laboratory_cross_reference/check/curie/{curie:path}
/laboratory_cross_reference/check/patterns
/laboratory_cross_reference/laboratory/{curie_or_laboratory_id}
/laboratory_cross_reference/{laboratory_cross_reference_id}
/laboratory_person/laboratory/{curie_or_laboratory_id}
/laboratory_person/person/{curie_or_person_id}
/laboratory_person/{laboratory_person_id}
/manual_indexing_tag/get_manual_indexing_tag/{reference_curie}/{mod_abbreviation}
/manual_indexing_tag/{manual_indexing_tag_id}
/ml_model/all
/ml_model/download/{task_type}/{mod_abbreviation}/{topic}
/ml_model/download/{task_type}/{mod_abbreviation}/{topic}/{version}
/ml_model/metadata/{task_type}/{mod_abbreviation}/{topic}
/ml_model/metadata/{task_type}/{mod_abbreviation}/{topic}/{version}
/mod/taxons/{type}
/mod/{abbreviation}
/mod/{mod_id}/versions
/ontology/entity_validation/{taxon}/{entity_type}/{entity_list:path}
/ontology/get_or_create_species/{taxon_id}
/ontology/map_curie_to_name/{category}/{curie}
/ontology/search_descendants/{ancestor_curie}
/ontology/search_descendants/{ancestor_curie}/{direct_children_only}/{include_self}/{include_names}
/ontology/search_species/{species}
/ontology/search_topic/{topic}
/openapi.json
/person/by_email/{email}
/person/by_name
/person/by_person_cross_reference/{curie_or_person_cross_reference_id}
/person/whoami
/person/{curie_or_person_id}
/person_cross_reference/check/curie/{curie:path}
/person_cross_reference/check/patterns
/person_cross_reference/person/{curie_or_person_id}
/person_cross_reference/{person_cross_reference_id}
/person_email/person/{curie_or_person_id}
/person_email/{person_email_id}
/person_lineage/person/{curie_or_person_id}
/person_lineage/{person_lineage_id}
/person_lineage_submission/person/{curie_or_person_id}
/person_lineage_submission/{person_lineage_submission_id}
/person_name/person/{curie_or_person_id}
/person_name/{person_name_id}
/person_note/person/{curie_or_person_id}
/person_note/{person_note_id}
/person_setting/by_email/{email}
/person_setting/by_name
/person_setting/{person_setting_id}
/redoc
/reference/by_cross_reference/{curie_or_cross_reference_id}
/reference/download_tracker_table/{mod_abbreviation}
/reference/dumps/latest/{mod}
/reference/dumps/{filename}
/reference/embedding_file/{embedding_file_id}
/reference/external_lookup/{external_curie}
/reference/get_bib_info/{curie}
/reference/get_recently_deleted_references/{mod_abbreviation}
/reference/get_recently_sorted_pmids/{mod_abbreviation}
/reference/get_recently_sorted_references/{mod_abbreviation}
/reference/get_textpresso_reference_list/{mod_abbreviation}
/reference/lock_status/{referenceCurie}
/reference/mesh_detail/{mesh_detail_id}
/reference/mesh_detail/{mesh_detail_id}/versions
/reference/missing_files/{mod_abbreviation}
/reference/mod_corpus_association/reference/{curie}/mod_abbreviation/{mod_abbreviation}
/reference/mod_corpus_association/{mod_corpus_association_id}
/reference/mod_corpus_association/{mod_corpus_association_id}/versions
/reference/mod_reference_type/by_mod/{abbreviation}
/reference/mod_reference_type/utils/mod_reftype_to_mods
/reference/mod_reference_type/{mod_reference_type_id}
/reference/mod_reference_type/{mod_reference_type_id}/versions
/reference/obsolete_mod_curies/{mod_abbreviation}
/reference/referencefile/additional_files_tarball/{reference_id}
/reference/referencefile/by_md5/{md5sum}
/reference/referencefile/conversion_request/{curie_or_reference_id}
/reference/referencefile/download_file/{referencefile_id}
/reference/referencefile/show_all/{curie_or_reference_id}
/reference/referencefile/{referencefile_id}
/reference/referencefile_mod/{referencefile_mod_id}
/reference/{curie_or_reference_id}
/reference/{curie_or_reference_id}/emails
/reference/{curie_or_reference_id}/image_permission
/reference/{curie_or_reference_id}/versions
/reference_relation/{reference_relation_id}
/reference_relation/{reference_relation_id}/versions
/resource/external_lookup/{external_curie}
/resource/show_all
/resource/{curie}
/resource/{curie}/versions
/resource_descriptor/
/sort/need_review
/sort/need_review/sort_sources
/sort/prepublication_pipeline
/sort/recently_sorted
/topic_entity_tag/by_mod/{mod_abbreviation}
/topic_entity_tag/by_reference/{curie_or_reference_id}
/topic_entity_tag/get_curie_to_name_from_all_tets/
/topic_entity_tag/revalidate_all_tags/
/topic_entity_tag/source/all
/topic_entity_tag/source/{source_evidence_assertion}/{source_method}/{data_provider}/{secondary_data_provider_abbreviation}
/topic_entity_tag/source/{topic_entity_tag_source_id}
/topic_entity_tag/{topic_entity_tag_id}
/vocabulary/
/vocabulary/{name}
/vocabulary/{name}/autocomplete
/vocabulary_abc/{vocabulary_abc_id}
/vocabulary_term_abc/{vocabulary_term_abc_id}
/vocabulary_term_synonym_abc/{vocabulary_term_synonym_abc_id}
/workflow_tag/by_mod/{mod_abbreviation}
/workflow_tag/counters/
/workflow_tag/get_current_workflow_status/{curie_or_reference_id}/{mod_abbreviation}/{workflow_process_atp_id}
/workflow_tag/get_name/{workflow_tag_id}
/workflow_tag/indexing-community/{reference_curie}
/workflow_tag/indexing-community/{reference_curie}/{mod_abbreviation}
/workflow_tag/jobs/{job_string}
/workflow_tag/pre_curation_overview/{reference_curie}
/workflow_tag/reports/{workflow_tag_id}/{mod_abbreviation}
/workflow_tag/subsets/{workflow_name}/{mod_abbreviation}
/workflow_tag/workflow_diagram/{mod}
/workflow_tag/{reference_workflow_tag_id}
/workflow_tag/{reference_workflow_tag_id}/versions
```
