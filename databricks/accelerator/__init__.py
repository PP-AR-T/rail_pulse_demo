"""Puente streaming decision accelerator: the domain-agnostic core.

    sensors -> minute aggregates -> pack detectors -> incidents -> agent card -> human decision

Everything domain-specific lives in a domain pack (domain_packs/*.yml). This package must not
import or mention any one domain. Pure-Python modules (contracts, packs, agent) have no Spark
dependency so they can be unit-tested anywhere; spark_ops holds the DataFrame logic shared by
the simulator notebook and the declarative pipeline.
"""
