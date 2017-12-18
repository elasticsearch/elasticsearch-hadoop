/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.rest;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.util.BytesArray;

/**
 * Simple response object that tracks useful information about a bulk indexing response.
 * This includes the determined response code, the number of docs indexed, the docs that
 * did not make it, and a sample of error messages for the user to peruse.
 */
public class BulkResponse {

    public static BulkResponse complete() {
        return complete(HttpStatus.OK, 0L, 0);
    }

    public static BulkResponse complete(int httpStatus, long spent, int totalWrites) {
        return new BulkResponse(BulkStatus.COMPLETE, httpStatus, spent, totalWrites, Collections.<BulkError>emptyList());
    }

    public static BulkResponse partial(int httpStatus, long spent, int totalWrites, List<BulkError> errors) {
        return new BulkResponse(BulkStatus.PARTIAL, httpStatus, spent, totalWrites, errors);
    }

    public enum BulkStatus {
        /**
         * The bulk operation was completed successfully with all documents accepted
         */
        COMPLETE,
        /**
         * The bulk operation was completed partially, with some documents containing failures
         */
        PARTIAL
    }

    public static class BulkError {

        private final int originalPosition;
        private final int currentArrayPosition;
        private final BytesArray document;
        private final int documentStatus;
        private final String errorMessage;

        public BulkError(int originalPosition, int currentArrayPosition, BytesArray document, int documentStatus, String errorMessage) {
            this.originalPosition = originalPosition;
            this.currentArrayPosition = currentArrayPosition;
            this.document = document;
            this.documentStatus = documentStatus;
            this.errorMessage = errorMessage;
        }

        /**
         * @return original location in tracking bytes array that the document existed in for this request.
         */
        public int getOriginalPosition() {
            return originalPosition;
        }

        /**
         * @return current location in the tracking bytes array that the document source exists in for this request.
         */
        public int getCurrentArrayPosition() {
            return currentArrayPosition;
        }

        /**
         * @return the document in original form, backed by the tracking bytes array.
         */
        public BytesArray getDocument() {
            return document;
        }

        public int getDocumentStatus() {
            return documentStatus;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    private final BulkStatus status;
    private final int httpStatus;
    private final long spent;
    private final int totalDocs;
    private List<BulkError> documentErrors;

    private BulkResponse(BulkStatus status, int httpStatus, long spent, int totalDocs, List<BulkError> documentErrors) {
        this.status = status;
        this.httpStatus = httpStatus;
        this.spent = spent;
        this.totalDocs = totalDocs;
        this.documentErrors = documentErrors;
    }

    public BulkStatus getStatus() {
        return status;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public long getClientTimeSpent() {
        return spent;
    }

    public int getTotalDocs() {
        return totalDocs;
    }

    public List<BulkError> getDocumentErrors() {
        return documentErrors;
    }
}
