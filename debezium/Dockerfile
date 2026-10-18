# Debezium with Confluent's JSON Schema converter (R9): every value's schema is registered in the Schema Registry
# under BACKWARD compatibility, so a breaking change at the source stops the connector (and alerts) instead of
# reaching consumers. Messages stay JSON, prefixed by the registry's 5-byte header (magic byte + schema id).
FROM debezium/connect:2.7.3.Final
ARG CONVERTER_VERSION=7.9.0
ARG CONVERTER_SHA256=74ca5f0a44921c2832e4e944a010aafc6c1e4ad7ff132df96394e3da000031b9
RUN curl -sSfL -o /tmp/converter.zip \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-json-schema-converter/versions/${CONVERTER_VERSION}/confluentinc-kafka-connect-json-schema-converter-${CONVERTER_VERSION}.zip" \
 && echo "${CONVERTER_SHA256}  /tmp/converter.zip" | sha256sum -c - \
 && unzip -q /tmp/converter.zip -d /kafka/connect \
 && rm /tmp/converter.zip
