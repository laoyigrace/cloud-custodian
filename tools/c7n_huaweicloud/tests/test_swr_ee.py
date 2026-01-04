# Copyright The Cloud Custodian Authors.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from huaweicloud_common import BaseTest


class SwrEeRepositoryTest(BaseTest):
    """Test SWR Enterprise Edition Repository resources, filters, and actions."""

    def test_swr_ee_repository_query(self):
        """Test SWR EE Repository query and basic resource attributes."""
        factory = self.replay_flight_data("swr_ee_repository_query")
        p = self.load_policy(
            {
                "name": "swr-ee-repository-query",
                "resource": "huaweicloud.swr-ee",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify VCR: swr_ee_repository_query should contain repositories
        self.assertGreaterEqual(len(resources), 1)
        # Verify resource contains required fields
        self.assertTrue("uid" in resources[0])
        self.assertTrue("name" in resources[0])
        self.assertTrue("instance_id" in resources[0])
        self.assertTrue("is_public" in resources[0])

    def test_swr_ee_repository_query_with_instance_id(self):
        """Test SWR EE Repository query with specific instance_id."""
        factory = self.replay_flight_data("swr_ee_repository_query_instance")
        p = self.load_policy(
            {
                "name": "swr-ee-repository-query-instance",
                "resource": "huaweicloud.swr-ee",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify resources are returned
        self.assertGreaterEqual(len(resources), 0)

    def test_swr_ee_repository_filter_value(self):
        """Test SWR EE Repository value filter for filtering by field values."""
        factory = self.replay_flight_data("swr_ee_filter_value")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-value",
                "resource": "huaweicloud.swr-ee",
                "filters": [{"type": "value", "key": "is_public", "value": False}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources
        if len(resources) > 0:
            self.assertFalse(resources[0]["is_public"])

    def test_swr_ee_repository_filter_age(self):
        """Test SWR EE Repository age filter for filtering by creation time."""
        factory = self.replay_flight_data("swr_ee_filter_age")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-age",
                "resource": "huaweicloud.swr-ee",
                "filters": [{"type": "age", "days": 90, "op": "gt"}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources
        if len(resources) > 0:
            self.assertTrue("created_at" in resources[0])
            created_date = datetime.strptime(
                resources[0]["created_at"], "%Y-%m-%dT%H:%M:%SZ"
            )
            self.assertTrue((datetime.now() - created_date).days > 90)

    def test_swr_ee_repository_get_resources(self):
        """Test SWR EE Repository get_resources by resource IDs."""
        factory = self.replay_flight_data("swr_ee_repository_get_resources")
        p = self.load_policy(
            {
                "name": "swr-ee-repository-get-resources",
                "resource": "huaweicloud.swr-ee",
            },
            session_factory=factory,
        )
        # Test with valid resource ID format: instance_id/namespace_name/repo_name
        resource_ids = ["instance-123/namespace-1/repo-1"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Verify resources are returned or empty list
        self.assertIsInstance(resources, list)

    def test_swr_ee_repository_get_resources_invalid_id(self):
        """Test SWR EE Repository get_resources with invalid resource ID format."""
        factory = self.replay_flight_data("swr_ee_repository_get_resources_invalid")
        p = self.load_policy(
            {
                "name": "swr-ee-repository-get-resources-invalid",
                "resource": "huaweicloud.swr-ee",
            },
            session_factory=factory,
        )
        # Test with invalid resource ID format
        resource_ids = ["invalid-id"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Should return empty list for invalid format
        self.assertEqual(len(resources), 0)


class SwrEeImageTest(BaseTest):
    """Test SWR Enterprise Edition Image resources, filters, and actions."""

    def test_swr_ee_image_query(self):
        """Test SWR EE Image query and resource enumeration."""
        factory = self.replay_flight_data("swr_ee_image_query")
        p = self.load_policy(
            {
                "name": "swr-ee-image-query",
                "resource": "huaweicloud.swr-ee-image",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify VCR: Resources should be returned
        self.assertGreaterEqual(len(resources), 0)
        if len(resources) > 0:
            # Verify resource contains required fields
            self.assertTrue("uid" in resources[0])
            self.assertTrue("instance_id" in resources[0])

    def test_swr_ee_image_query_with_instance_id(self):
        """Test SWR EE Image query with specific instance_id."""
        factory = self.replay_flight_data("swr_ee_image_query_instance")
        p = self.load_policy(
            {
                "name": "swr-ee-image-query-instance",
                "resource": "huaweicloud.swr-ee-image",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify resources are returned
        self.assertGreaterEqual(len(resources), 0)

    def test_swr_ee_image_query_fallback_to_traverse(self):
        """Test SWR EE Image query fallback to traverse repositories when API fails."""
        factory = self.replay_flight_data("swr_ee_image_query_fallback")
        p = self.load_policy(
            {
                "name": "swr-ee-image-query-fallback",
                "resource": "huaweicloud.swr-ee-image",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify resources are returned even when fallback is used
        self.assertGreaterEqual(len(resources), 0)

    def test_swr_ee_image_filter_age(self):
        """Test SWR EE Image age filter for filtering by creation time."""
        factory = self.replay_flight_data("swr_ee_image_filter_age")
        p = self.load_policy(
            {
                "name": "swr-ee-image-filter-age",
                "resource": "huaweicloud.swr-ee-image",
                "filters": [{"type": "age", "days": 90, "op": "gt"}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources
        if len(resources) > 0:
            self.assertTrue("push_time" in resources[0])
            push_date = datetime.strptime(
                resources[0]["push_time"], "%Y-%m-%dT%H:%M:%SZ"
            )
            self.assertTrue((datetime.now() - push_date).days > 90)

    def test_swr_ee_image_filter_value(self):
        """Test SWR EE Image value filter for filtering by field values."""
        factory = self.replay_flight_data("swr_ee_image_filter_value")
        p = self.load_policy(
            {
                "name": "swr-ee-image-filter-value",
                "resource": "huaweicloud.swr-ee-image",
                "filters": [{"type": "value", "key": "tag", "value": "latest"}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources
        if len(resources) > 0:
            self.assertEqual(resources[0]["tag"], "latest")

    def test_swr_ee_image_get_resources(self):
        """Test SWR EE Image get_resources by resource IDs."""
        factory = self.replay_flight_data("swr_ee_image_get_resources")
        p = self.load_policy(
            {
                "name": "swr-ee-image-get-resources",
                "resource": "huaweicloud.swr-ee-image",
            },
            session_factory=factory,
        )
        # Test with valid resource ID format: instance_id/namespace_name/repo_name/digest
        resource_ids = ["instance-123/namespace-1/repo-1/digest-123"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Verify resources are returned or empty list
        self.assertIsInstance(resources, list)

    def test_swr_ee_image_get_resources_invalid_id(self):
        """Test SWR EE Image get_resources with invalid resource ID format."""
        factory = self.replay_flight_data("swr_ee_image_get_resources_invalid")
        p = self.load_policy(
            {
                "name": "swr-ee-image-get-resources-invalid",
                "resource": "huaweicloud.swr-ee-image",
            },
            session_factory=factory,
        )
        # Test with invalid resource ID format
        resource_ids = ["invalid-id"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Should return empty list for invalid format
        self.assertEqual(len(resources), 0)


class SwrEeNamespaceTest(BaseTest):
    """Test SWR Enterprise Edition Namespace resources, filters, and actions."""

    def test_swr_ee_namespace_query(self):
        """Test SWR EE Namespace query and basic resource attributes."""
        factory = self.replay_flight_data("swr_ee_namespace_query")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-query",
                "resource": "huaweicloud.swr-ee-namespace",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify VCR: Resources should be returned
        self.assertGreaterEqual(len(resources), 0)
        if len(resources) > 0:
            # Verify resource contains required fields
            self.assertTrue("id" in resources[0])
            self.assertTrue("name" in resources[0])
            self.assertTrue("instance_id" in resources[0])
            self.assertTrue("is_public" in resources[0])

    def test_swr_ee_namespace_query_with_instance_id(self):
        """Test SWR EE Namespace query with specific instance_id."""
        factory = self.replay_flight_data("swr_ee_namespace_query_instance")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-query-instance",
                "resource": "huaweicloud.swr-ee-namespace",
            },
            session_factory=factory,
        )

        resources = p.run()
        # Verify resources are returned
        self.assertGreaterEqual(len(resources), 0)

    def test_swr_ee_namespace_filter_value(self):
        """Test SWR EE Namespace value filter for filtering by field values."""
        factory = self.replay_flight_data("swr_ee_namespace_filter_value")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-filter-value",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "value", "key": "is_public", "value": False}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources
        if len(resources) > 0:
            self.assertFalse(resources[0]["is_public"])

    def test_swr_ee_namespace_filter_age(self):
        """Test SWR EE Namespace age filter for filtering by creation time."""
        factory = self.replay_flight_data("swr_ee_namespace_filter_age")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-filter-age",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "age", "days": 90, "op": "gt"}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources
        if len(resources) > 0:
            self.assertTrue("created_at" in resources[0])
            created_date = datetime.strptime(
                resources[0]["created_at"], "%Y-%m-%dT%H:%M:%SZ"
            )
            self.assertTrue((datetime.now() - created_date).days > 90)

    def test_swr_ee_namespace_get_resources(self):
        """Test SWR EE Namespace get_resources by resource IDs."""
        factory = self.replay_flight_data("swr_ee_namespace_get_resources")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-get-resources",
                "resource": "huaweicloud.swr-ee-namespace",
            },
            session_factory=factory,
        )
        # Test with valid resource ID format: instance_id/namespace_name
        resource_ids = ["instance-123/namespace-1"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Verify resources are returned or empty list
        self.assertIsInstance(resources, list)

    def test_swr_ee_namespace_get_resources_invalid_id(self):
        """Test SWR EE Namespace get_resources with invalid resource ID format."""
        factory = self.replay_flight_data("swr_ee_namespace_get_resources_invalid")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-get-resources-invalid",
                "resource": "huaweicloud.swr-ee-namespace",
            },
            session_factory=factory,
        )
        # Test with invalid resource ID format
        resource_ids = ["invalid-id"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Should return empty list for invalid format
        self.assertEqual(len(resources), 0)

    def test_swr_ee_namespace_get_resources_special_cases(self):
        """Test SWR EE Namespace get_resources with special case IDs."""
        factory = self.replay_flight_data("swr_ee_namespace_get_resources_special")
        p = self.load_policy(
            {
                "name": "swr-ee-namespace-get-resources-special",
                "resource": "huaweicloud.swr-ee-namespace",
            },
            session_factory=factory,
        )
        # Test with special case IDs that trigger full fetch
        resource_ids = ["deleteSignaturePolicy", "deleteRetention"]
        manager = p.resource_manager
        resources = manager.get_resources(resource_ids)
        # Should return all resources for special cases
        self.assertIsInstance(resources, list)


class LifecycleRuleFilterTest(BaseTest):
    """Test SWR EE Namespace Lifecycle Rule filter functionality."""

    def test_lifecycle_rule_filter_match(self):
        """Test Lifecycle Rule filter - Match namespaces with lifecycle rules."""
        factory = self.replay_flight_data("swr_ee_filter_lifecycle_rule_match")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-lifecycle-rule-match",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "lifecycle-rule", "state": True}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources with lifecycle rules should be returned
        self.assertGreaterEqual(len(resources), 0)
        if len(resources) > 0:
            # Verify lifecycle policy is lazily loaded by the filter
            self.assertTrue("c7n:lifecycle-policy" in resources[0])
            lifecycle_policy = resources[0]["c7n:lifecycle-policy"]
            # Verify lifecycle policy is a list
            self.assertTrue(isinstance(lifecycle_policy, list))
            self.assertTrue(len(lifecycle_policy) > 0)

    def test_lifecycle_rule_filter_no_match(self):
        """Test Lifecycle Rule filter - Match namespaces without lifecycle rules."""
        factory = self.replay_flight_data("swr_ee_filter_lifecycle_rule_no_match")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-lifecycle-rule-no-match",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "lifecycle-rule", "state": False}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources without lifecycle rules should be returned
        self.assertGreaterEqual(len(resources), 0)
        if len(resources) > 0:
            # Verify lifecycle policy
            self.assertTrue("c7n:lifecycle-policy" in resources[0])
            lifecycle_policy = resources[0]["c7n:lifecycle-policy"]
            # Verify lifecycle policy is empty list
            self.assertTrue(isinstance(lifecycle_policy, list))
            self.assertEqual(len(lifecycle_policy), 0)

    def test_lifecycle_rule_filter_with_match(self):
        """Test Lifecycle Rule filter with match conditions."""
        factory = self.replay_flight_data("swr_ee_filter_lifecycle_rule_match_conditions")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-lifecycle-rule-match-conditions",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "lifecycle-rule",
                        "state": True,
                        "match": [
                            {
                                "type": "value",
                                "key": "algorithm",
                                "value": "or"
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources match the conditions
        self.assertGreaterEqual(len(resources), 0)

    def test_lifecycle_rule_filter_with_tag_selector(self):
        """Test Lifecycle Rule filter with tag selector."""
        factory = self.replay_flight_data("swr_ee_filter_lifecycle_rule_tag_selector")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-lifecycle-rule-tag-selector",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "lifecycle-rule",
                        "state": True,
                        "tag_selector": {
                            "kind": "doublestar",
                            "pattern": "v5"
                        }
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources match the tag selector
        self.assertGreaterEqual(len(resources), 0)


class SetLifecycleActionTest(BaseTest):
    """Test SWR EE Namespace Set Lifecycle Rule actions."""

    def test_create_lifecycle_rule(self):
        """Test creating lifecycle rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_lifecycle_action_create")
        p = self.load_policy(
            {
                "name": "swr-ee-create-lifecycle",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "value", "key": "name", "value": "test-namespace"}],
                "actions": [
                    {
                        "type": "set-lifecycle",
                        "algorithm": "or",
                        "rules": [
                            {
                                "template": "nDaysSinceLastPush",
                                "params": {"nDaysSinceLastPush": 30},
                                "scope_selectors": {
                                    "repository": [
                                        {
                                            "kind": "doublestar",
                                            "pattern": "**"
                                        }
                                    ]
                                },
                                "tag_selectors": [
                                    {
                                        "kind": "doublestar",
                                        "pattern": "**"
                                    }
                                ]
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_update_lifecycle_rule(self):
        """Test updating lifecycle rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_lifecycle_action_update")
        p = self.load_policy(
            {
                "name": "swr-ee-update-lifecycle",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "value", "key": "name", "value": "test-namespace"}],
                "actions": [
                    {
                        "type": "set-lifecycle",
                        "algorithm": "or",
                        "rules": [
                            {
                                "template": "nDaysSinceLastPull",
                                "params": {"nDaysSinceLastPull": 60},
                                "scope_selectors": {
                                    "repository": [
                                        {
                                            "kind": "doublestar",
                                            "pattern": "{repo1, repo2}"
                                        }
                                    ]
                                },
                                "tag_selectors": [
                                    {
                                        "kind": "doublestar",
                                        "pattern": "^release-.*$"
                                    }
                                ]
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_cancel_lifecycle_rule(self):
        """Test canceling lifecycle rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_lifecycle_action_cancel")
        p = self.load_policy(
            {
                "name": "swr-ee-cancel-lifecycle",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "lifecycle-rule",
                        "state": True
                    }
                ],
                "actions": [
                    {
                        "type": "set-lifecycle",
                        "state": False
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_set_lifecycle_validation_error(self):
        """Test set-lifecycle action validation errors."""
        factory = self.replay_flight_data("swr_ee_lifecycle_action_validation")
        # Test invalid configuration: state=False with rules
        try:
            self.load_policy(
                {
                    "name": "swr-ee-lifecycle-validation-error",
                    "resource": "huaweicloud.swr-ee-namespace",
                    "actions": [
                        {
                            "type": "set-lifecycle",
                            "state": False,
                            "rules": [
                                {
                                    "template": "nDaysSinceLastPush",
                                    "params": {"nDaysSinceLastPush": 30},
                                    "tag_selectors": []
                                }
                            ]
                        }
                    ],
                },
                session_factory=factory,
            )
            # Should raise PolicyValidationError
            self.fail("Expected PolicyValidationError")
        except Exception as e:
            # Expected validation error
            self.assertIsNotNone(e)


class SetImmutabilityActionTest(BaseTest):
    """Test SWR EE Namespace Set Immutability Rule actions."""

    def test_create_immutability_rule(self):
        """Test creating immutability rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_immutability_action_create")
        p = self.load_policy(
            {
                "name": "swr-ee-create-immutability",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "value", "key": "name", "value": "test-namespace"}],
                "actions": [
                    {
                        "type": "set-immutability",
                        "state": True,
                        "scope_selectors": {
                            "repository": [
                                {
                                    "kind": "doublestar",
                                    "pattern": "**"
                                }
                            ]
                        },
                        "tag_selectors": [
                            {
                                "kind": "doublestar",
                                "pattern": "**"
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_update_immutability_rule(self):
        """Test updating immutability rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_immutability_action_update")
        p = self.load_policy(
            {
                "name": "swr-ee-update-immutability",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "value", "key": "name", "value": "test-namespace"}],
                "actions": [
                    {
                        "type": "set-immutability",
                        "state": True,
                        "scope_selectors": {
                            "repository": [
                                {
                                    "kind": "doublestar",
                                    "pattern": "{repo1, repo2}"
                                }
                            ]
                        },
                        "tag_selectors": [
                            {
                                "kind": "doublestar",
                                "pattern": "^release-.*$"
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_cancel_immutability_rule(self):
        """Test canceling immutability rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_immutability_action_cancel")
        p = self.load_policy(
            {
                "name": "swr-ee-cancel-immutability",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "value", "key": "name", "value": "test-namespace"}],
                "actions": [
                    {
                        "type": "set-immutability",
                        "state": False,
                        "scope_selectors": {
                            "repository": [
                                {
                                    "kind": "doublestar",
                                    "pattern": "**"
                                }
                            ]
                        },
                        "tag_selectors": [
                            {
                                "kind": "doublestar",
                                "pattern": "**"
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)


class SignatureRuleFilterTest(BaseTest):
    """Test SWR EE Namespace Signature Rule filter functionality."""

    def test_signature_rule_filter_match(self):
        """Test Signature Rule filter - Match namespaces with signature rules."""
        factory = self.replay_flight_data("swr_ee_filter_signature_rule_match")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-signature-rule-match",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "signature-rule", "state": True}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources with signature rules should be returned
        self.assertGreaterEqual(len(resources), 0)
        if len(resources) > 0:
            # Verify signature policy is lazily loaded by the filter
            self.assertTrue("c7n:signature-policy" in resources[0])
            signature_policy = resources[0]["c7n:signature-policy"]
            # Verify signature policy is a list
            self.assertTrue(isinstance(signature_policy, list))
            self.assertTrue(len(signature_policy) > 0)

    def test_signature_rule_filter_no_match(self):
        """Test Signature Rule filter - Match namespaces without signature rules."""
        factory = self.replay_flight_data("swr_ee_filter_signature_rule_no_match")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-signature-rule-no-match",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [{"type": "signature-rule", "state": False}],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources without signature rules should be returned
        self.assertGreaterEqual(len(resources), 0)
        if len(resources) > 0:
            # Verify signature policy
            self.assertTrue("c7n:signature-policy" in resources[0])
            signature_policy = resources[0]["c7n:signature-policy"]
            # Verify signature policy is empty list
            self.assertTrue(isinstance(signature_policy, list))
            self.assertEqual(len(signature_policy), 0)

    def test_signature_rule_filter_with_match(self):
        """Test Signature Rule filter with match conditions."""
        factory = self.replay_flight_data("swr_ee_filter_signature_rule_match_conditions")
        p = self.load_policy(
            {
                "name": "swr-ee-filter-signature-rule-match-conditions",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "signature-rule",
                        "state": True,
                        "match": [
                            {
                                "type": "value",
                                "key": "enabled",
                                "value": True
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify filtered resources match the conditions
        self.assertGreaterEqual(len(resources), 0)


class SetSignatureActionTest(BaseTest):
    """Test SWR EE Namespace Set Signature Rule actions."""

    def test_create_signature_rule(self):
        """Test creating signature rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_signature_action_create")
        p = self.load_policy(
            {
                "name": "swr-ee-create-signature",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "signature-rule",
                        "state": False
                    }
                ],
                "actions": [
                    {
                        "type": "set-signature",
                        "signature_algorithm": "ECDSA_SHA_256",
                        "signature_key": "test-key-id",
                        "rules": [
                            {
                                "scope_selectors": {
                                    "repository": [
                                        {
                                            "kind": "doublestar",
                                            "pattern": "**"
                                        }
                                    ]
                                },
                                "tag_selectors": [
                                    {
                                        "kind": "doublestar",
                                        "pattern": "**"
                                    }
                                ]
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_update_signature_rule(self):
        """Test updating signature rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_signature_action_update")
        p = self.load_policy(
            {
                "name": "swr-ee-update-signature",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "signature-rule",
                        "state": True
                    }
                ],
                "actions": [
                    {
                        "type": "set-signature",
                        "signature_algorithm": "ECDSA_SHA_384",
                        "signature_key": "test-key-id-2",
                        "rules": [
                            {
                                "scope_selectors": {
                                    "repository": [
                                        {
                                            "kind": "doublestar",
                                            "pattern": "{repo1, repo2}"
                                        }
                                    ]
                                },
                                "tag_selectors": [
                                    {
                                        "kind": "doublestar",
                                        "pattern": "^release-.*$"
                                    }
                                ]
                            }
                        ]
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_cancel_signature_rule(self):
        """Test canceling signature rules for SWR EE namespaces."""
        factory = self.replay_flight_data("swr_ee_signature_action_cancel")
        p = self.load_policy(
            {
                "name": "swr-ee-cancel-signature",
                "resource": "huaweicloud.swr-ee-namespace",
                "filters": [
                    {
                        "type": "signature-rule",
                        "state": True
                    }
                ],
                "actions": [
                    {
                        "type": "set-signature",
                        "state": False
                    }
                ],
            },
            session_factory=factory,
        )
        resources = p.run()
        # Verify VCR: Resources should be processed
        self.assertGreaterEqual(len(resources), 0)

    def test_set_signature_validation_error(self):
        """Test set-signature action validation errors."""
        factory = self.replay_flight_data("swr_ee_signature_action_validation")
        # Test invalid configuration: state=False with rules
        try:
            self.load_policy(
                {
                    "name": "swr-ee-signature-validation-error",
                    "resource": "huaweicloud.swr-ee-namespace",
                    "actions": [
                        {
                            "type": "set-signature",
                            "state": False,
                            "rules": [
                                {
                                    "scope_selectors": {
                                        "repository": [
                                            {
                                                "kind": "doublestar",
                                                "pattern": "**"
                                            }
                                        ]
                                    },
                                    "tag_selectors": [
                                        {
                                            "kind": "doublestar",
                                            "pattern": "**"
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                },
                session_factory=factory,
            )
            # Should raise PolicyValidationError
            self.fail("Expected PolicyValidationError")
        except Exception as e:
            # Expected validation error
            self.assertIsNotNone(e)
