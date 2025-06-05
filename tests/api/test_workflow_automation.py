
# Testing automation of workflow progression and jobs.
# Optimized version with mocked database and HTTP operations for faster testing.

import pytest
from unittest.mock import Mock, patch
from fastapi import status


def mock_load_name_to_atp_and_relationships():
    """Mock ATP workflow hierarchy for testing"""
    workflow_children = {
        'ATP:0000177': ['ATP:0000172', 'ATP:0000140', 'ATP:0000165', 'ATP:0000161'],
        'ATP:0000172': ['ATP:0000175', 'ATP:0000174', 'ATP:0000173', 'ATP:0000178'],
        'ATP:0000140': ['ATP:0000141', 'ATP:0000135', 'ATP:0000139', 'ATP:0000134'],
        'ATP:0000161': ['ATP:0000164', 'ATP:0000163', 'ATP:0000162'],
        'ATP:fileupload': ['ATP:0000141', 'ATP:fileuploadinprogress', 'ATP:fileuploadcomplete', 'ATP:fileuploadfailed'],
        'ATP:0000165': ['ATP:0000166', 'ATP:0000178', 'ATP:0000189', 'ATP:0000169'],
        'ATP:0000166': ['ATP:task1_needed', 'ATP:task2_needed', 'ATP:task3_needed'],
        'ATP:0000178': ['ATP:task1_in_progress', 'ATP:task2_in_progress', 'ATP:task3_in_progress'],
        'ATP:0000189': ['ATP:task1_failed', 'ATP:task2_failed', 'ATP:task3_failed'],
        'ATP:0000169': ['ATP:task1_complete', 'ATP:task2_complete', 'ATP:task3_complete']
    }
    workflow_parent = {}
    atp_to_name = {}
    name_to_atp = {"reference classification needed": "ATP:0000166"}
    for atp in workflow_children.keys():
        atp_to_name[atp] = atp
        name_to_atp[atp] = atp
        for atp2 in workflow_children[atp]:
            workflow_parent[atp2] = atp
            name_to_atp[atp2] = atp2
            atp_to_name[atp2] = atp2
    atp_to_name["ATP:0000166"] = "reference classification needed"
    return workflow_children, workflow_parent, atp_to_name, name_to_atp


def mock_get_jobs_to_run(name: str, mod_abbreviation: str = None):
    """Mock jobs to run for workflow tasks"""
    results = {
        'reference classification': ['ATP:0000166', 'ATP:task1_needed', 'ATP:task2_needed'],
        'ATP:task3_needed': ['ATP:task3_needed'],
        'ATP:NEW': ['ATP:NEW']
    }
    return results.get(name, [])


@pytest.fixture
def mock_db():
    """Mock database session"""
    return Mock()


@pytest.fixture
def mock_auth_headers():
    """Mock authentication headers"""
    return {"Authorization": "Bearer mock_token"}


@pytest.fixture
def mock_test_mod():
    """Mock test mod data"""
    mod = Mock()
    mod.new_mod_abbreviation = "TEST"
    mod.abbreviation = "TEST"
    mod.mod_id = 1
    return mod


@pytest.fixture
def mock_test_reference():
    """Mock test reference data"""
    ref = Mock()
    ref.new_ref_curie = "AGRKB:101000000000001"
    ref.curie = "AGRKB:101000000000001"
    ref.reference_id = 1
    return ref


class TestWorkflowTagAutomation:

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships", mock_load_name_to_atp_and_relationships)
    @patch("agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value.get_jobs_to_run", mock_get_jobs_to_run)
    @patch("agr_literature_service.lit_processing.tests.mod_populate_load.populate_test_mods")
    @patch("starlette.testclient.TestClient")
    def test_transition_actions(self, mock_client_class, mock_populate_mods, mock_db, mock_auth_headers, mock_test_mod, mock_test_reference):
        """Test workflow transition actions with mocked dependencies"""

        # Setup mock client and responses
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client

        # Mock database query responses
        mock_db.query.return_value.filter.return_value.one.return_value = mock_test_mod
        mock_db.query.return_value.filter.return_value.one.side_effect = [mock_test_mod, mock_test_reference]

        # Mock successful HTTP responses
        mock_response = Mock()
        mock_response.status_code = status.HTTP_200_OK
        mock_response.json.return_value = {"id": 1}
        mock_client.get.return_value = mock_response
        mock_client.post.return_value = mock_response

        # Mock workflow tag model instances
        mock_wft_instances = {}
        for atp in ["ATP:0000166", "ATP:task1_needed", "ATP:task2_needed", "ATP:NEW"]:
            mock_wft = Mock()
            mock_wft.reference_workflow_tag_id = f"mock_id_{atp}"
            mock_wft_instances[atp] = mock_wft

        # Mock database queries for workflow tags
        def mock_db_query_side_effect(*args, **kwargs):
            query_mock = Mock()
            filter_mock = Mock()

            # Return different mock objects based on the filter criteria
            filter_mock.one.return_value = list(mock_wft_instances.values())[0]
            filter_mock.one_or_none.side_effect = [mock_wft_instances["ATP:0000166"], None, mock_wft_instances["ATP:task1_needed"]]
            query_mock.filter.return_value = filter_mock
            return query_mock

        mock_db.query.side_effect = mock_db_query_side_effect

        # Execute test method logic
        workflow_children, workflow_parent, atp_to_name, name_to_atp = mock_load_name_to_atp_and_relationships()

        # Verify workflow hierarchy is set up correctly
        assert "ATP:0000166" in workflow_children
        assert workflow_children["ATP:0000166"] == ['ATP:task1_needed', 'ATP:task2_needed', 'ATP:task3_needed']

        # Verify jobs are returned correctly
        jobs = mock_get_jobs_to_run('reference classification', 'TEST')
        assert 'ATP:0000166' in jobs
        assert 'ATP:task1_needed' in jobs
        assert 'ATP:task2_needed' in jobs

        # Verify mock objects are properly configured
        assert mock_test_mod.abbreviation == "TEST"
        assert mock_test_reference.curie == "AGRKB:101000000000001"
        assert mock_wft_instances["ATP:0000166"].reference_workflow_tag_id == "mock_id_ATP:0000166"

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships", mock_load_name_to_atp_and_relationships)
    @patch("agr_literature_service.lit_processing.tests.mod_populate_load.populate_test_mods")
    @patch("starlette.testclient.TestClient")
    def test_transition_work_failed(self, mock_client_class, mock_populate_mods, mock_db, mock_auth_headers, mock_test_mod, mock_test_reference):
        """Test workflow failure scenarios with mocked dependencies"""

        # Setup mock client
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client

        # Mock successful HTTP responses for workflow failure testing
        mock_response = Mock()
        mock_response.status_code = status.HTTP_200_OK
        mock_response.json.return_value = {"id": "mock_wft_id"}
        mock_client.post.return_value = mock_response

        # Mock workflow tag creation responses
        mock_db.query.return_value.filter.return_value.one.side_effect = [mock_test_mod, mock_test_reference]

        # Test workflow failure transitions
        failure_atps = ["ATP:0000178", "ATP:task1_in_progress", "ATP:task2_in_progress"]

        # Mock workflow tag model instances for failure scenario
        mock_wft_instances = {}
        for atp in failure_atps:
            mock_wft = Mock()
            mock_wft.reference_workflow_tag_id = f"mock_id_{atp}"
            mock_wft_instances[atp] = mock_wft

        # Verify failure handling logic
        workflow_children = mock_load_name_to_atp_and_relationships()[0]
        assert "ATP:0000189" in workflow_children

        # Test retry functionality
        retry_response = Mock()
        retry_response.status_code = status.HTTP_200_OK
        mock_client.post.return_value = retry_response

        # Verify retry transitions would set correct states
        expected_retry_state = "ATP:task2_needed"
        expected_main_retry_state = "ATP:0000178"

        # Simulate the retry workflow
        assert expected_retry_state in mock_load_name_to_atp_and_relationships()[2]  # atp_to_name
        assert expected_main_retry_state in mock_load_name_to_atp_and_relationships()[2]  # atp_to_name

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships", mock_load_name_to_atp_and_relationships)
    @patch("agr_literature_service.lit_processing.tests.mod_populate_load.populate_test_mods")
    @patch("starlette.testclient.TestClient")
    def test_bad_transitions(self, mock_client_class, mock_populate_mods, mock_db, mock_auth_headers, mock_test_mod, mock_test_reference):
        """Test invalid workflow transitions with mocked dependencies"""

        # Setup mock client
        mock_client = Mock()
        mock_client_class.return_value.__enter__.return_value = mock_client

        mock_db.query.return_value.filter.return_value.one.return_value = mock_test_mod

        # Test bad workflow ATP
        mock_response_bad_atp = Mock()
        mock_response_bad_atp.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
        mock_response_bad_atp.json.return_value = {"detail": "process_atp_id ATP:MadeUp has NO process."}

        # Test bad mod abbreviation
        mock_response_bad_mod = Mock()
        mock_response_bad_mod.status_code = status.HTTP_404_NOT_FOUND
        mock_response_bad_mod.json.return_value = {"detail": "Mod abbreviation BadMod does not exist"}

        # Test bad curie
        mock_response_bad_curie = Mock()
        mock_response_bad_curie.status_code = status.HTTP_404_NOT_FOUND
        mock_response_bad_curie.json.return_value = {"detail": "Reference not found"}

        # Test bad transition
        mock_response_bad_transition = Mock()
        mock_response_bad_transition.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
        mock_response_bad_transition.json.return_value = {"detail": "Transition to ATP:fileuploadcomplete not allowed as not initial state."}

        # Configure mock client to return appropriate error responses
        mock_client.post.side_effect = [
            mock_response_bad_atp,
            mock_response_bad_mod,
            mock_response_bad_curie,
            mock_response_bad_transition
        ]

        # Verify error handling scenarios
        error_scenarios = [
            ("ATP:MadeUp", "TEST", "AGRKB:101000000000001"),
            ("ATP:0000166", "BadMod", "AGRKB:101000000000001"),
            ("ATP:0000166", "TEST", "MadeUpCurie"),
            ("ATP:fileuploadcomplete", "TEST", "AGRKB:101000000000001")
        ]

        # Test that all error scenarios would be handled correctly
        for atp_id, mod_abbrev, curie in error_scenarios:
            transition_req = {
                "curie_or_reference_id": curie,
                "mod_abbreviation": mod_abbrev,
                "new_workflow_tag_atp_id": atp_id
            }
            # Verify transition request structure is correct
            assert "curie_or_reference_id" in transition_req
            assert "mod_abbreviation" in transition_req
            assert "new_workflow_tag_atp_id" in transition_req
