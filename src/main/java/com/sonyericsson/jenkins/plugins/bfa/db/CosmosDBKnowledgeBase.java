package com.sonyericsson.jenkins.plugins.bfa.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;
import com.sonyericsson.jenkins.plugins.bfa.statistics.Statistics;
import hudson.Extension;
import hudson.Util;
import hudson.model.Descriptor;
import hudson.util.FormValidation;
import jenkins.model.Jenkins;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;
import rx.Observable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class CosmosDBKnowledgeBase extends KnowledgeBase {

    private static final String FAILURE_CAUSE_COLLECTION = "failureCause";
    private static final String STATISTICS_COLLECTION = "statistics";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String url;
    private String database;
    private String key;
    private boolean enableStatistics;
    private boolean successfulLogging;

    private transient AsyncDocumentClient documentClient;
    private transient String failureCauseCollectionLink;
    private transient String statisticsCollectionLink;

    @DataBoundConstructor
    public CosmosDBKnowledgeBase(
            String url,
            String database,
            String key,
            boolean enableStatistics,
            boolean successfulLogging
    ) {
        this.url = "https://tim-temp2.documents.azure.com:443";
        this.database = "TestDB";
        this.key = "av0VQNAPPYtFHSzxBmbixu4QrIEW5TH8HyoBLBouQ3PIalkYuYQAiZG3QPG1kD3rwIXznxgR6KgPcqOUlsT8cA==";
        this.enableStatistics = true;
        this.successfulLogging = false;
        this.statisticsCollectionLink = "dbs/" + this.database + "/colls/" + STATISTICS_COLLECTION;
        this.failureCauseCollectionLink = "dbs/" + this.database + "/colls/" + FAILURE_CAUSE_COLLECTION;
    }

    @Override
    public Collection<FailureCause> getCauses() {
        // TODO inline
        List<FailureCause> collect = asStream(
                getClient()
                        .readDocuments(failureCauseCollectionLink, null)
                        .toBlocking()
                        .getIterator())
                .flatMap(r -> r.getResults().stream())
                .map(d -> d.toObject(FailureCause.class))
                .collect(toList());
        return collect;
    }

    private static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    @Override
    public Collection<FailureCause> getCauseNames() throws Exception {
        return null;
    }

    @Override
    public Collection<FailureCause> getShallowCauses() throws Exception {
        return null;
    }

    @Override
    public FailureCause getCause(String id) throws Exception {
        return null;
    }

    @Override
    public FailureCause addCause(FailureCause cause) throws Exception {
        return saveCause(cause);
    }

    @Override
    public FailureCause removeCause(String id) throws Exception {
        return null;
    }

    @Override
    public FailureCause saveCause(FailureCause cause) throws Exception {
        Document doc = fromObject(cause);

        return getClient().createDocument(failureCauseCollectionLink, doc, null, false)
                .single()
                .toBlocking()
                    .first()
                .getResource()
                .toObject(FailureCause.class);
    }

    @Override
    public void convertFrom(KnowledgeBase oldKnowledgeBase) throws Exception {

    }

    @Override
    public List<String> getCategories() throws Exception {
        return null;
    }

    @Override
    public boolean equals(KnowledgeBase oldKnowledgeBase) {
        return false;
    }

    private AsyncDocumentClient getClient() {
        if (documentClient == null) {
            documentClient = initClient();
        }

        return documentClient;
    }

    private AsyncDocumentClient initClient() {
        documentClient = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(url)
                .withMasterKeyOrResourceToken(key)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Eventual)
                .build();

        Database databaseDefinition = new Database();
        databaseDefinition.setId(database);

        documentClient.readDatabase("dbs/" + database, null)
                .onErrorReturn((throwable) -> null)
                .flatMap(res -> {
                    DocumentCollection statisticsCollection = new DocumentCollection();
                    statisticsCollection.setId(STATISTICS_COLLECTION);
                    return documentClient.createCollection("dbs/" + database, statisticsCollection, null);
                })
                .onErrorReturn((throwable) -> null)
                .flatMap(res -> {
                    DocumentCollection failureCauseCollection = new DocumentCollection();
                    failureCauseCollection.setId(FAILURE_CAUSE_COLLECTION);

                    return documentClient.createCollection("dbs/" + database, failureCauseCollection, null);
                })
                .onErrorReturn((throwable) -> null)
                .toBlocking()
                .getIterator()
                .next();

        return documentClient;
    }

    @Override
    public void start() {
        initClient();
    }

    @Override
    public void stop() {
        if (documentClient != null) {
            documentClient.close();
        }
    }

    @Override
    public boolean isStatisticsEnabled() {
        return enableStatistics;
    }

    @Override
    public boolean isSuccessfulLoggingEnabled() {
        return successfulLogging;
    }

    @Override
    public void saveStatistics(Statistics stat) throws Exception {
        Document doc = fromObject(stat);

        Observable<ResourceResponse<Document>> createDocumentObservable =
                getClient().createDocument(STATISTICS_COLLECTION, doc, null, false);
        createDocumentObservable
                .single()           // we know there will be one response
                .subscribe(
                        documentResourceResponse -> {
                            System.out.println(documentResourceResponse.getRequestCharge());
                        },

                        error -> {
                            System.err.println("an error happened: " + error.getMessage());
                        });
    }

    @Override
    public Descriptor<KnowledgeBase> getDescriptor() {
        return Jenkins.getInstance().getDescriptorByType(CosmosDBKnowledgeBaseDescriptor.class);
    }

    private Document fromObject(Object document) {
        try {
            return new Document(OBJECT_MAPPER.writeValueAsString(document));
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't serialize the object into the json string", e);
        }
    }

    /**
     * Descriptor for {@link MongoDBKnowledgeBase}.
     */
    @Extension
    public static class CosmosDBKnowledgeBaseDescriptor extends KnowledgeBaseDescriptor {

        @Override
        public String getDisplayName() {
            // TODO use localisation api
            return "Cosmos DB";
        }

        /**
         * Checks that the host name is not empty.
         *
         * @param value the pattern to check.
         * @return {@link hudson.util.FormValidation#ok()} if everything is well.
         */
        public FormValidation doCheckHost(@QueryParameter("value") final String value) {
            if (Util.fixEmpty(value) == null) {
                return FormValidation.error("Please provide a host name!");
            } else {
                try {
                    new URI(value);
                } catch (URISyntaxException e) {
                    return FormValidation.error(e.getMessage());
                }
            }
            return FormValidation.ok();
        }

        /**
         * Checks that the database name is not empty.
         *
         * @param value the database name to check.
         * @return {@link hudson.util.FormValidation#ok()} if everything is well.
         */
        public FormValidation doCheckDBName(@QueryParameter("value") String value) {
            if (Util.fixEmpty(value) == null) {
                return FormValidation.error("Please provide a database name!");
            } else {
                Matcher m = Pattern.compile("\\s").matcher(value);
                if (m.find()) {
                    return FormValidation.error("Database name contains white space!");
                }
                return FormValidation.ok();
            }
        }

        /**
         * Tests if the provided parameters can connect to the Mongo database.
         *
         * @param host   the host name.
         * @param dbName the database name.
         * @return {@link FormValidation#ok() } if can be done,
         * {@link FormValidation#error(java.lang.String) } otherwise.
         */
        public FormValidation doTestConnection(
                @QueryParameter("host") final String host,
                @QueryParameter("dbName") final String dbName,
                @QueryParameter("key") final String key
        ) {
            // TODO use localisation api
            return FormValidation.ok("All is good");
        }
    }

}
