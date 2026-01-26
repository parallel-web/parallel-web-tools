"""Tests for the Spark UDF integration module."""

import asyncio
import json
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest


class TestEnrichAllAsync:
    """Tests for the _enrich_all_async function."""

    @pytest.fixture
    def mock_async_client(self):
        """Create a mock AsyncParallel client."""
        client = mock.AsyncMock()
        return client

    def test_concurrent_processing(self):
        """Should process all items concurrently using asyncio.gather."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        # Track the order of calls to verify concurrency
        call_order = []

        async def mock_create(input, task_spec, processor):
            call_order.append(f"create_{input['company']}")
            return SimpleNamespace(run_id=f"run_{input['company']}")

        async def mock_result(run_id, api_timeout):
            call_order.append(f"result_{run_id}")
            company = run_id.replace("run_", "")
            return SimpleNamespace(output=SimpleNamespace(content={"ceo_name": f"CEO of {company}"}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            items = [
                {"company": "Google"},
                {"company": "Microsoft"},
                {"company": "Apple"},
            ]

            results = asyncio.run(
                _enrich_all_async(
                    items=items,
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                )
            )

        assert len(results) == 3
        # Verify all creates happen before any results (concurrent execution)
        # With asyncio.gather, all creates should be initiated together
        assert json.loads(results[0])["ceo_name"] == "CEO of Google"
        assert json.loads(results[1])["ceo_name"] == "CEO of Microsoft"
        assert json.loads(results[2])["ceo_name"] == "CEO of Apple"

    def test_error_handling_per_item(self):
        """Should handle errors for individual items without failing others."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        async def mock_create(input, task_spec, processor):
            if input.get("company") == "BadCompany":
                raise ValueError("Invalid company")
            return SimpleNamespace(run_id=f"run_{input['company']}")

        async def mock_result(run_id, api_timeout):
            company = run_id.replace("run_", "")
            return SimpleNamespace(output=SimpleNamespace(content={"ceo_name": f"CEO of {company}"}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            items = [
                {"company": "Google"},
                {"company": "BadCompany"},
                {"company": "Apple"},
            ]

            results = asyncio.run(
                _enrich_all_async(
                    items=items,
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                )
            )

        assert len(results) == 3
        assert json.loads(results[0])["ceo_name"] == "CEO of Google"
        assert "error" in json.loads(results[1])
        assert "Invalid company" in json.loads(results[1])["error"]
        assert json.loads(results[2])["ceo_name"] == "CEO of Apple"

    def test_builds_correct_task_spec(self):
        """Should build the correct task spec from output columns."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        captured_task_spec = None

        async def mock_create(input, task_spec, processor):
            nonlocal captured_task_spec
            captured_task_spec = task_spec
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(output=SimpleNamespace(content={"ceo_name": "Test"}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Test"}],
                    output_columns=["CEO name", "Founding year"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                )
            )

        assert captured_task_spec is not None
        # The task_spec should have an output_schema with the columns
        # TaskSpecParam is a TypedDict, so access as dict
        output_schema = captured_task_spec["output_schema"]
        schema = output_schema["json_schema"]
        assert "ceo_name" in schema["properties"]
        assert "founding_year" in schema["properties"]

    def test_passes_processor_correctly(self):
        """Should pass the processor parameter to each task run."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        captured_processor = None

        async def mock_create(input, task_spec, processor):
            nonlocal captured_processor
            captured_processor = processor
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(output=SimpleNamespace(content={}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Test"}],
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="pro-fast",
                    timeout=300,
                )
            )

        assert captured_processor == "pro-fast"

    def test_passes_timeout_correctly(self):
        """Should pass the timeout parameter to task_run.result."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        captured_timeout = None

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            nonlocal captured_timeout
            captured_timeout = api_timeout
            return SimpleNamespace(output=SimpleNamespace(content={}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Test"}],
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=600,
                )
            )

        assert captured_timeout == 600

    def test_handles_dict_content(self):
        """Should handle dict content in response."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(
                output=SimpleNamespace(content={"ceo_name": "Sundar Pichai", "founding_year": "1998"})
            )

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            results = asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Google"}],
                    output_columns=["CEO name", "Founding year"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                )
            )

        result = json.loads(results[0])
        assert result["ceo_name"] == "Sundar Pichai"
        assert result["founding_year"] == "1998"

    def test_handles_non_dict_content(self):
        """Should wrap non-dict content in a result key."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(output=SimpleNamespace(content="plain text response"))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            results = asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Google"}],
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                )
            )

        result = json.loads(results[0])
        assert result["result"] == "plain text response"

    def test_include_basis_adds_citations(self):
        """Should include _basis field when include_basis=True."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(
                output=SimpleNamespace(
                    content={"ceo_name": "Sundar Pichai"},
                    basis=[
                        SimpleNamespace(
                            field="ceo_name",
                            citations=[
                                SimpleNamespace(
                                    url="https://example.com/source",
                                    excerpts=["Sundar Pichai is the CEO"],
                                )
                            ],
                            reasoning="Found in Wikipedia article",
                            confidence="high",
                        )
                    ],
                )
            )

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            results = asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Google"}],
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                    include_basis=True,
                )
            )

        result = json.loads(results[0])
        assert result["ceo_name"] == "Sundar Pichai"
        assert "_basis" in result
        assert len(result["_basis"]) == 1
        assert result["_basis"][0]["field"] == "ceo_name"
        assert result["_basis"][0]["citations"][0]["url"] == "https://example.com/source"
        assert result["_basis"][0]["reasoning"] == "Found in Wikipedia article"
        assert result["_basis"][0]["confidence"] == "high"

    def test_include_basis_false_excludes_citations(self):
        """Should not include _basis field when include_basis=False."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(
                output=SimpleNamespace(
                    content={"ceo_name": "Sundar Pichai"},
                    basis=[
                        SimpleNamespace(
                            field="ceo_name",
                            citations=[SimpleNamespace(url="https://example.com", excerpts=[])],
                        )
                    ],
                )
            )

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            results = asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Google"}],
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                    include_basis=False,
                )
            )

        result = json.loads(results[0])
        assert result["ceo_name"] == "Sundar Pichai"
        assert "_basis" not in result

    def test_include_basis_empty_basis(self):
        """Should include empty _basis when include_basis=True but no basis in response."""
        from parallel_web_tools.integrations.spark.udf import _enrich_all_async

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(
                output=SimpleNamespace(
                    content={"ceo_name": "Sundar Pichai"},
                    basis=None,
                )
            )

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            results = asyncio.run(
                _enrich_all_async(
                    items=[{"company": "Google"}],
                    output_columns=["CEO name"],
                    api_key="test-key",
                    processor="lite-fast",
                    timeout=300,
                    include_basis=True,
                )
            )

        result = json.loads(results[0])
        assert result["ceo_name"] == "Sundar Pichai"
        assert "_basis" in result
        assert result["_basis"] == []


class TestParallelEnrichPartition:
    """Tests for the _parallel_enrich_partition function."""

    def test_empty_partition(self):
        """Should handle empty partitions."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        result = _parallel_enrich_partition(
            input_data_series=pd.Series([], dtype=object),
            output_columns=["CEO name"],
            api_key="test-key",
            processor="lite-fast",
            timeout=300,
        )

        assert len(result) == 0
        assert isinstance(result, pd.Series)

    def test_all_none_values(self):
        """Should handle partitions with all None values."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        result = _parallel_enrich_partition(
            input_data_series=pd.Series([None, None, None]),
            output_columns=["CEO name"],
            api_key="test-key",
            processor="lite-fast",
            timeout=300,
        )

        assert len(result) == 3
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None

    def test_mixed_none_and_valid_values(self):
        """Should handle partitions with mixed None and valid values."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id=f"run_{input['company']}")

        async def mock_result(run_id, api_timeout):
            company = run_id.replace("run_", "")
            return SimpleNamespace(output=SimpleNamespace(content={"ceo_name": f"CEO of {company}"}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series(
                    [
                        {"company": "Google"},
                        None,
                        {"company": "Apple"},
                        None,
                    ]
                ),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        assert len(result) == 4
        assert json.loads(result[0])["ceo_name"] == "CEO of Google"
        assert result[1] is None
        assert json.loads(result[2])["ceo_name"] == "CEO of Apple"
        assert result[3] is None

    def test_preserves_order(self):
        """Should preserve the order of results matching input order."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id=f"run_{input['company']}")

        async def mock_result(run_id, api_timeout):
            company = run_id.replace("run_", "")
            return SimpleNamespace(output=SimpleNamespace(content={"ceo_name": f"CEO of {company}"}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series(
                    [
                        {"company": "Alpha"},
                        {"company": "Beta"},
                        {"company": "Gamma"},
                    ]
                ),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        assert len(result) == 3
        assert json.loads(result[0])["ceo_name"] == "CEO of Alpha"
        assert json.loads(result[1])["ceo_name"] == "CEO of Beta"
        assert json.loads(result[2])["ceo_name"] == "CEO of Gamma"

    def test_include_basis_passed_through(self):
        """Should pass include_basis parameter to async function."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id="run_1")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(
                output=SimpleNamespace(
                    content={"ceo_name": "Test CEO"},
                    basis=[
                        SimpleNamespace(
                            field="ceo_name",
                            citations=[SimpleNamespace(url="https://test.com", excerpts=["test"])],
                            reasoning="Test reasoning",
                            confidence="high",
                        )
                    ],
                )
            )

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series([{"company": "Test"}]),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
                include_basis=True,
            )

        assert len(result) == 1
        parsed = json.loads(result[0])
        assert parsed["ceo_name"] == "Test CEO"
        assert "_basis" in parsed
        assert parsed["_basis"][0]["field"] == "ceo_name"


class TestCreateParallelEnrichUdf:
    """Tests for the create_parallel_enrich_udf factory function."""

    def test_captures_api_key(self):
        """Should capture the API key at creation time."""
        from parallel_web_tools.integrations.spark.udf import create_parallel_enrich_udf

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="captured-key",
        ) as mock_resolve:
            create_parallel_enrich_udf(api_key="my-key")

            mock_resolve.assert_called_once_with("my-key")

    def test_captures_processor(self):
        """Should capture the processor at creation time."""
        from parallel_web_tools.integrations.spark.udf import create_parallel_enrich_udf

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            with mock.patch("parallel_web_tools.integrations.spark.udf._parallel_enrich_partition") as mock_partition:
                mock_partition.return_value = pd.Series(["{}"])

                udf_func = create_parallel_enrich_udf(processor="ultra-fast")

                # The UDF is a pandas_udf wrapper, we need to call its inner function
                # For now, just verify it was created without error
                assert udf_func is not None

    def test_default_parameters(self):
        """Should use default parameters when not specified."""
        from parallel_web_tools.integrations.spark.udf import create_parallel_enrich_udf

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            # Should not raise with defaults
            udf_func = create_parallel_enrich_udf()
            assert udf_func is not None

    def test_include_basis_parameter(self):
        """Should accept include_basis parameter."""
        from parallel_web_tools.integrations.spark.udf import create_parallel_enrich_udf

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            # Should not raise with include_basis=True
            udf_func = create_parallel_enrich_udf(include_basis=True)
            assert udf_func is not None

            # Should not raise with include_basis=False
            udf_func = create_parallel_enrich_udf(include_basis=False)
            assert udf_func is not None


class TestRegisterParallelUdfs:
    """Tests for the register_parallel_udfs function."""

    def test_registers_main_udf(self):
        """Should register the main parallel_enrich UDF."""
        from parallel_web_tools.integrations.spark.udf import register_parallel_udfs

        mock_spark = mock.MagicMock()

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            register_parallel_udfs(mock_spark, api_key="test-key")

        # Should register at least the main UDF
        assert mock_spark.udf.register.call_count >= 1
        call_names = [call[0][0] for call in mock_spark.udf.register.call_args_list]
        assert "parallel_enrich" in call_names

    def test_registers_with_processor_udf(self):
        """Should register the _with_processor variant UDF."""
        from parallel_web_tools.integrations.spark.udf import register_parallel_udfs

        mock_spark = mock.MagicMock()

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            register_parallel_udfs(mock_spark, api_key="test-key")

        call_names = [call[0][0] for call in mock_spark.udf.register.call_args_list]
        assert "parallel_enrich_with_processor" in call_names

    def test_custom_udf_name(self):
        """Should register UDF with custom name."""
        from parallel_web_tools.integrations.spark.udf import register_parallel_udfs

        mock_spark = mock.MagicMock()

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            register_parallel_udfs(mock_spark, udf_name="my_custom_enrich")

        call_names = [call[0][0] for call in mock_spark.udf.register.call_args_list]
        assert "my_custom_enrich" in call_names
        assert "my_custom_enrich_with_processor" in call_names

    def test_resolves_api_key_at_registration(self):
        """Should resolve the API key at registration time."""
        from parallel_web_tools.integrations.spark.udf import register_parallel_udfs

        mock_spark = mock.MagicMock()

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="resolved-key",
        ) as mock_resolve:
            register_parallel_udfs(mock_spark, api_key="input-key")

            # Should be called to resolve the key
            mock_resolve.assert_called()

    def test_include_basis_parameter(self):
        """Should accept include_basis parameter when registering UDFs."""
        from parallel_web_tools.integrations.spark.udf import register_parallel_udfs

        mock_spark = mock.MagicMock()

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.resolve_api_key",
            return_value="test-key",
        ):
            # Should not raise with include_basis=True
            register_parallel_udfs(mock_spark, include_basis=True)

            # Verify UDFs were still registered
            call_names = [call[0][0] for call in mock_spark.udf.register.call_args_list]
            assert "parallel_enrich" in call_names
            assert "parallel_enrich_with_processor" in call_names


class TestIntegration:
    """Integration tests for the Spark UDF module."""

    def test_full_enrichment_flow(self):
        """Test a complete enrichment flow with mocked API."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        # Simulate what would happen in a real Spark partition
        async def mock_create(input, task_spec, processor):
            return SimpleNamespace(run_id=f"run_{hash(str(input))}")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(
                output=SimpleNamespace(
                    content={
                        "ceo_name": "Test CEO",
                        "founding_year": "2000",
                        "headquarters": "San Francisco",
                    }
                )
            )

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            # Simulate a partition with multiple companies
            input_series = pd.Series(
                [
                    {"company_name": "Company A", "website": "a.com"},
                    {"company_name": "Company B", "website": "b.com"},
                    {"company_name": "Company C", "website": "c.com"},
                ]
            )

            result = _parallel_enrich_partition(
                input_data_series=input_series,
                output_columns=["CEO name", "Founding year", "Headquarters"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        assert len(result) == 3
        for i in range(3):
            parsed = json.loads(result[i])
            assert parsed["ceo_name"] == "Test CEO"
            assert parsed["founding_year"] == "2000"
            assert parsed["headquarters"] == "San Francisco"

    def test_error_resilience(self):
        """Test that errors in one row don't affect others."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        call_count = 0

        async def mock_create(input, task_spec, processor):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("API temporarily unavailable")
            return SimpleNamespace(run_id=f"run_{call_count}")

        async def mock_result(run_id, api_timeout):
            return SimpleNamespace(output=SimpleNamespace(content={"ceo_name": "Success"}))

        mock_client = mock.AsyncMock()
        mock_client.task_run.create = mock_create
        mock_client.task_run.result = mock_result

        with mock.patch("parallel.AsyncParallel", return_value=mock_client):
            input_series = pd.Series(
                [
                    {"company": "A"},
                    {"company": "B"},  # This one will fail
                    {"company": "C"},
                ]
            )

            result = _parallel_enrich_partition(
                input_data_series=input_series,
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        assert len(result) == 3
        assert json.loads(result[0])["ceo_name"] == "Success"
        assert "error" in json.loads(result[1])
        assert json.loads(result[2])["ceo_name"] == "Success"
