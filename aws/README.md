# MDF Connect AWS backend

## Index migration

CloudFormation permits only one DynamoDB global secondary index create or delete per stack update. Roll out each changed submissions-table index in two deployments: first remove the old index or projection definition, then add the replacement definition from `template.yaml`. Apply this sequence separately for `status-submissions` → `curation-queue-index` and for each `ALL` → `INCLUDE` projection change; do not deploy all index mutations in one stack update. No application deployment should occur between removing `status-submissions` and creating `curation-queue-index`, because the curation queue depends on the replacement index.
