package io.apicurio.registry.resolver;

import io.apicurio.registry.resolver.client.RegistryArtifactReference;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.resolver.data.Record;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractSchemaResolverTest {
    @Test
    void testConfigureInitializesSchemaCache() throws Exception {
        Map<String, String> configs = Collections.singletonMap(SchemaResolverConfig.REGISTRY_URL,
                "http://localhost");

        try (TestAbstractSchemaResolver<Object, Object> resolver = new TestAbstractSchemaResolver<>()) {
            resolver.configure(configs, null);

            assertDoesNotThrow(() -> {
                resolver.schemaCache.checkInitialized();
            });
        }
    }

    @Test
    void testSupportsFailureTolerantSchemaCache() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SchemaResolverConfig.REGISTRY_URL, "http://localhost");
        configs.put(SchemaResolverConfig.FAULT_TOLERANT_REFRESH, true);

        try (TestAbstractSchemaResolver<Object, Object> resolver = new TestAbstractSchemaResolver<>()) {
            resolver.configure(configs, null);

            assertTrue(resolver.schemaCache.isFaultTolerantRefresh());
        }
    }

    @Test
    void testDefaultsToFailureTolerantSchemaCacheDisabled() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SchemaResolverConfig.REGISTRY_URL, "http://localhost");

        try (TestAbstractSchemaResolver<Object, Object> resolver = new TestAbstractSchemaResolver<>()) {
            resolver.configure(configs, null);

            assertFalse(resolver.schemaCache.isFaultTolerantRefresh());
        }
    }

    @Test
    void testDefaultsToCacheLatestEnabled() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SchemaResolverConfig.REGISTRY_URL, "http://localhost");

        try (TestAbstractSchemaResolver<Object, Object> resolver = new TestAbstractSchemaResolver<>()) {
            resolver.configure(configs, null);

            assertTrue(resolver.schemaCache.isCacheLatest());
        }
    }

    // --- resolveReferences tests (issue #7681) ---

    private static final String CHILD_SCHEMA = "{\"type\": \"record\", \"name\": \"Child\", \"fields\": []}";
    private static final String GRANDCHILD_SCHEMA = "{\"type\": \"record\", \"name\": \"Grandchild\", \"fields\": []}";

    /**
     * Test that nested references are resolved exactly once (no duplicate recursive calls).
     * Before the fix, resolveReferences(referenceReferences) was called twice per iteration,
     * doubling all downstream API calls exponentially.
     */
    @Test
    void testResolveReferencesNoDuplicateRecursiveCalls() throws Exception {
        ReferenceTrackingFacade facade = new ReferenceTrackingFacade();

        facade.registerSchema("default", "child-artifact", "1", CHILD_SCHEMA,
                List.of(RegistryArtifactReference.builder()
                        .name("grandchild-ref")
                        .groupId("default")
                        .artifactId("grandchild-artifact")
                        .version("1")
                        .build()));
        facade.registerSchema("default", "grandchild-artifact", "1", GRANDCHILD_SCHEMA, List.of());

        try (TestAbstractSchemaResolver<String, String> resolver = createResolverWithFacade(facade)) {
            List<RegistryArtifactReference> parentRefs = List.of(
                    RegistryArtifactReference.builder()
                            .name("child-ref")
                            .groupId("default")
                            .artifactId("child-artifact")
                            .version("1")
                            .build());

            Map<String, ParsedSchema<String>> result = resolver.resolveReferences(parentRefs);

            assertEquals(2, result.size());
            assertNotNull(result.get("child-ref"));
            assertNotNull(result.get("grandchild-ref"));

            // Grandchild should have been fetched exactly once (not twice as before the fix)
            assertEquals(1, facade.getSchemaByGAVCount("default", "grandchild-artifact", "1"));
        }
    }

    /**
     * Test that the schemaCache is used: resolving the same reference twice should only
     * trigger one API call because the second lookup is a cache hit.
     */
    @Test
    void testResolveReferencesCacheHitOnRepeatedGAV() throws Exception {
        ReferenceTrackingFacade facade = new ReferenceTrackingFacade();
        facade.registerSchema("default", "shared-ref", "1", CHILD_SCHEMA, List.of());

        try (TestAbstractSchemaResolver<String, String> resolver = createResolverWithFacade(facade)) {
            List<RegistryArtifactReference> refs = List.of(
                    RegistryArtifactReference.builder()
                            .name("shared-ref")
                            .groupId("default")
                            .artifactId("shared-ref")
                            .version("1")
                            .build());

            // First resolution — cache miss, triggers API call
            resolver.resolveReferences(refs);
            assertEquals(1, facade.getSchemaByGAVCount("default", "shared-ref", "1"));

            // Second resolution — should be a cache hit, no additional API call
            resolver.resolveReferences(refs);
            assertEquals(1, facade.getSchemaByGAVCount("default", "shared-ref", "1"));
        }
    }

    /**
     * Test that when two sibling references share a common nested reference,
     * the shared nested reference is only fetched once within a single invocation.
     */
    @Test
    void testResolveReferencesSharedNestedReferenceResolvedOnce() throws Exception {
        ReferenceTrackingFacade facade = new ReferenceTrackingFacade();

        RegistryArtifactReference sharedDep = RegistryArtifactReference.builder()
                .name("shared-dep")
                .groupId("default")
                .artifactId("shared-dep")
                .version("1")
                .build();

        facade.registerSchema("default", "sibling-a", "1", CHILD_SCHEMA, List.of(sharedDep));
        facade.registerSchema("default", "sibling-b", "1", CHILD_SCHEMA, List.of(sharedDep));
        facade.registerSchema("default", "shared-dep", "1", GRANDCHILD_SCHEMA, List.of());

        try (TestAbstractSchemaResolver<String, String> resolver = createResolverWithFacade(facade)) {
            List<RegistryArtifactReference> parentRefs = List.of(
                    RegistryArtifactReference.builder()
                            .name("sibling-a-ref").groupId("default")
                            .artifactId("sibling-a").version("1").build(),
                    RegistryArtifactReference.builder()
                            .name("sibling-b-ref").groupId("default")
                            .artifactId("sibling-b").version("1").build());

            Map<String, ParsedSchema<String>> result = resolver.resolveReferences(parentRefs);

            assertEquals(3, result.size());
            assertNotNull(result.get("sibling-a-ref"));
            assertNotNull(result.get("sibling-b-ref"));
            assertNotNull(result.get("shared-dep"));

            // shared-dep should have been fetched exactly once (cached after sibling-a resolution)
            assertEquals(1, facade.getSchemaByGAVCount("default", "shared-dep", "1"));
        }
    }

    private TestAbstractSchemaResolver<String, String> createResolverWithFacade(ReferenceTrackingFacade facade) {
        TestAbstractSchemaResolver<String, String> resolver = new TestAbstractSchemaResolver<>();
        resolver.setClientFacade(facade);

        Map<String, Object> configs = new HashMap<>();
        configs.put(SchemaResolverConfig.REGISTRY_URL, "http://localhost");
        resolver.configure(configs, new MockSchemaParser());

        return resolver;
    }

    // --- Test helpers ---

    class TestAbstractSchemaResolver<SCHEMA, DATA> extends AbstractSchemaResolver<SCHEMA, DATA> {

        @Override
        public SchemaLookupResult<SCHEMA> resolveSchema(Record<DATA> data) {
            throw new UnsupportedOperationException("Unimplemented method 'resolveSchema'");
        }

        @Override
        public SchemaLookupResult<SCHEMA> resolveSchemaByArtifactReference(ArtifactReference reference) {
            throw new UnsupportedOperationException(
                    "Unimplemented method 'resolveSchemaByArtifactReference'");
        }

        @Override
        public Map<String, ParsedSchema<SCHEMA>> resolveReferences(
                List<RegistryArtifactReference> artifactReferences) {
            return super.resolveReferences(artifactReferences);
        }
    }

    /**
     * A mock facade that tracks per-GAV call counts and supports configurable reference graphs.
     */
    static class ReferenceTrackingFacade implements RegistryClientFacade {

        private final Map<String, String> schemas = new HashMap<>();
        private final Map<String, List<RegistryArtifactReference>> references = new HashMap<>();
        private final Map<String, AtomicInteger> gavCallCounts = new HashMap<>();

        void registerSchema(String groupId, String artifactId, String version,
                            String content, List<RegistryArtifactReference> refs) {
            String key = gavKey(groupId, artifactId, version);
            schemas.put(key, content);
            references.put(key, refs);
        }

        int getSchemaByGAVCount(String groupId, String artifactId, String version) {
            AtomicInteger count = gavCallCounts.get(gavKey(groupId, artifactId, version));
            return count == null ? 0 : count.get();
        }

        private String gavKey(String groupId, String artifactId, String version) {
            return groupId + "/" + artifactId + "/" + version;
        }

        @Override
        public String getSchemaByGAV(String groupId, String artifactId, String version) {
            gavCallCounts.computeIfAbsent(gavKey(groupId, artifactId, version),
                    k -> new AtomicInteger(0)).incrementAndGet();
            return schemas.getOrDefault(gavKey(groupId, artifactId, version), "{}");
        }

        @Override
        public List<RegistryArtifactReference> getReferencesByGAV(String groupId, String artifactId,
                                                                   String version) {
            return references.getOrDefault(gavKey(groupId, artifactId, version), List.of());
        }

        @Override
        public String getSchemaByContentId(Long contentId) { return "{}"; }

        @Override
        public String getSchemaByGlobalId(long globalId, boolean dereferenced) { return "{}"; }

        @Override
        public String getSchemaByContentHash(String contentHash) { return "{}"; }

        @Override
        public List<RegistryArtifactReference> getReferencesByContentId(long contentId) {
            return List.of();
        }

        @Override
        public List<RegistryArtifactReference> getReferencesByGlobalId(long globalId) {
            return List.of();
        }

        @Override
        public List<RegistryArtifactReference> getReferencesByContentHash(String contentHash) {
            return List.of();
        }

        @Override
        public List<RegistryVersionCoordinates> searchVersionsByContent(String schemaString,
                String artifactType, ArtifactReference reference, boolean canonical) {
            return List.of();
        }

        @Override
        public RegistryVersionCoordinates createSchema(String artifactType, String groupId,
                String artifactId, String version, String autoCreateBehavior, boolean canonical,
                String schemaString, Set<RegistryArtifactReference> references) {
            return RegistryVersionCoordinates.create(1L, 1L, groupId, artifactId, "1");
        }

        @Override
        public RegistryVersionCoordinates getVersionCoordinatesByGAV(String groupId, String artifactId,
                                                                      String version) {
            return RegistryVersionCoordinates.create(1L, 1L, groupId, artifactId,
                    version != null ? version : "1");
        }

        @Override
        public Object getClient() { return null; }
    }
}
