import unittest

from dags.policy_ingestion import policy_ingestion


class PolicyIngestionDagTest(unittest.TestCase):
    def test_dag_parses_and_exchanges_artifact_paths(self):
        workflow = policy_ingestion()

        self.assertEqual(
            set(workflow.task_ids),
            {
                "wait_for_pdf",
                "extract_documents",
                "chunk_documents",
                "embed_chunks",
                "finalize_ingestion",
            },
        )
        self.assertIs(
            workflow.task_dict["extract_documents"].python_callable.__annotations__["return"],
            str,
        )
        self.assertIs(
            workflow.task_dict["chunk_documents"].python_callable.__annotations__["return"],
            str,
        )
