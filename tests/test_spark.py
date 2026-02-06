"""Tests for the Spark UDF integration module."""

import json
from unittest import mock

import pandas as pd


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

    def test_basic_enrichment(self):
        """Should enrich items via enrich_batch and return JSON strings."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [
            {"ceo_name": "Sundar Pichai"},
            {"ceo_name": "Satya Nadella"},
            {"ceo_name": "Tim Cook"},
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series(
                    [
                        {"company": "Google"},
                        {"company": "Microsoft"},
                        {"company": "Apple"},
                    ]
                ),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        assert len(result) == 3
        assert json.loads(result[0])["ceo_name"] == "Sundar Pichai"
        assert json.loads(result[1])["ceo_name"] == "Satya Nadella"
        assert json.loads(result[2])["ceo_name"] == "Tim Cook"

    def test_mixed_none_and_valid_values(self):
        """Should handle partitions with mixed None and valid values."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [
            {"ceo_name": "CEO of Google"},
            {"ceo_name": "CEO of Apple"},
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
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

        mock_results = [
            {"ceo_name": "CEO of Alpha"},
            {"ceo_name": "CEO of Beta"},
            {"ceo_name": "CEO of Gamma"},
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
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

    def test_chunking_large_batches(self):
        """Should chunk >1000 rows into multiple enrich_batch calls."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        num_items = 2500
        items = [{"company": f"Company_{i}"} for i in range(num_items)]

        def mock_enrich_batch(**kwargs):
            return [{"ceo_name": f"CEO_{i}"} for i in range(len(kwargs["inputs"]))]

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.enrich_batch",
            side_effect=mock_enrich_batch,
        ) as mock_batch:
            result = _parallel_enrich_partition(
                input_data_series=pd.Series(items),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        # 2500 items should produce 3 calls: 1000 + 1000 + 500
        assert mock_batch.call_count == 3
        assert len(mock_batch.call_args_list[0][1]["inputs"]) == 1000
        assert len(mock_batch.call_args_list[1][1]["inputs"]) == 1000
        assert len(mock_batch.call_args_list[2][1]["inputs"]) == 500
        assert len(result) == num_items

    def test_error_results_preserved_as_json(self):
        """Should preserve error dicts from enrich_batch as JSON strings."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [
            {"ceo_name": "Success CEO"},
            {"error": "API temporarily unavailable"},
            {"ceo_name": "Another CEO"},
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series(
                    [
                        {"company": "A"},
                        {"company": "B"},
                        {"company": "C"},
                    ]
                ),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        assert len(result) == 3
        assert json.loads(result[0])["ceo_name"] == "Success CEO"
        assert "error" in json.loads(result[1])
        assert "API temporarily unavailable" in json.loads(result[1])["error"]
        assert json.loads(result[2])["ceo_name"] == "Another CEO"

    def test_include_basis_renames_to_underscore(self):
        """Should rename 'basis' to '_basis' in output."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [
            {
                "ceo_name": "Sundar Pichai",
                "basis": [
                    {
                        "field": "ceo_name",
                        "citations": [{"url": "https://example.com/source", "excerpts": ["Sundar Pichai is the CEO"]}],
                        "reasoning": "Found in Wikipedia article",
                        "confidence": "high",
                    }
                ],
            }
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series([{"company": "Google"}]),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
                include_basis=True,
            )

        parsed = json.loads(result[0])
        assert parsed["ceo_name"] == "Sundar Pichai"
        assert "_basis" in parsed
        assert "basis" not in parsed
        assert len(parsed["_basis"]) == 1
        assert parsed["_basis"][0]["field"] == "ceo_name"
        assert parsed["_basis"][0]["citations"][0]["url"] == "https://example.com/source"
        assert parsed["_basis"][0]["reasoning"] == "Found in Wikipedia article"
        assert parsed["_basis"][0]["confidence"] == "high"

    def test_include_basis_false_no_basis_key(self):
        """Should not include basis when include_basis=False."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [{"ceo_name": "Sundar Pichai"}]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series([{"company": "Google"}]),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
                include_basis=False,
            )

        parsed = json.loads(result[0])
        assert parsed["ceo_name"] == "Sundar Pichai"
        assert "_basis" not in parsed
        assert "basis" not in parsed

    def test_non_dict_result_handling(self):
        """Should wrap non-dict results in a result key."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = ["plain text response"]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
            result = _parallel_enrich_partition(
                input_data_series=pd.Series([{"company": "Google"}]),
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
            )

        parsed = json.loads(result[0])
        assert parsed["result"] == "plain text response"

    def test_passes_parameters_to_enrich_batch(self):
        """Should pass all parameters correctly to enrich_batch."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        with mock.patch(
            "parallel_web_tools.integrations.spark.udf.enrich_batch",
            return_value=[{"ceo_name": "Test"}],
        ) as mock_batch:
            _parallel_enrich_partition(
                input_data_series=pd.Series([{"company": "Test"}]),
                output_columns=["CEO name"],
                api_key="my-api-key",
                processor="pro-fast",
                timeout=600,
                include_basis=True,
            )

        mock_batch.assert_called_once_with(
            inputs=[{"company": "Test"}],
            output_columns=["CEO name"],
            api_key="my-api-key",
            processor="pro-fast",
            timeout=600,
            include_basis=True,
            source="spark",
        )


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
        """Test a complete enrichment flow with mocked enrich_batch."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [
            {"ceo_name": "Test CEO", "founding_year": "2000", "headquarters": "San Francisco"},
            {"ceo_name": "Test CEO", "founding_year": "2000", "headquarters": "San Francisco"},
            {"ceo_name": "Test CEO", "founding_year": "2000", "headquarters": "San Francisco"},
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
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
        """Test that error results from enrich_batch are preserved."""
        from parallel_web_tools.integrations.spark.udf import _parallel_enrich_partition

        mock_results = [
            {"ceo_name": "Success"},
            {"error": "API temporarily unavailable"},
            {"ceo_name": "Success"},
        ]

        with mock.patch("parallel_web_tools.integrations.spark.udf.enrich_batch", return_value=mock_results):
            input_series = pd.Series(
                [
                    {"company": "A"},
                    {"company": "B"},
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
