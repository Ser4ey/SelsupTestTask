package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bucket4j.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;


@Log4j2
public class CrptApi {
    private static final String BASE_URL = "https://ismp.crpt.ru/api/v3";
    private static final String CREATE_DOCUMENTS_URL = BASE_URL + "/lk/documents/create";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Bucket bucket;

    public CrptApi(int maxRequests, Duration refillDuration) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();

        Bandwidth limit = Bandwidth.classic(
                maxRequests,
                Refill.intervally(maxRequests, refillDuration)
        );
        this.bucket = Bucket4j.builder()
                .addLimit(limit)
                .build();
    }

    public void applyRateLimit() {
        // блокирует поток при превышении лимита запрос до тех пор пока лимит не востановится
        while (true) {
            ConsumptionProbe probe = bucket.tryConsumeAndReturnRemaining(1);

            if (probe.isConsumed()) {
                // Вызываем метод, если лимит не достигнут
                log.debug("Вызов API выполняется...");
                log.debug("Осталось запросов: " + probe.getRemainingTokens());
                return ;
            } else {
                // Если лимит достигнут, ждем пополнения
                long waitForRefillNanos = probe.getNanosToWaitForRefill();
                long waitForRefillMillis = waitForRefillNanos / 1_000_000;
                log.debug("Лимит достигнут. Ожидание: " + waitForRefillMillis + " мс.");
                try {
                    Thread.sleep(waitForRefillMillis);
                } catch (InterruptedException e) {
                    log.error("Вызов API был прерван.", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public HttpResponse<String> createDocuments(Document document, String signature) throws IOException, InterruptedException {
        // лимит на запросы
        applyRateLimit();

        String requestBody = objectMapper.writeValueAsString(document);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(CREATE_DOCUMENTS_URL))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        log.debug("Запрос: {} | Headers: {}", request, request.headers());

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        log.debug("Код ответа: {} | Ответ: {}", response.statusCode(), response.body());

        return response;
    }

    public static void main(String[] args) {
        Document document = new Document();
        document.setDoc_id("123456789");
        document.setDoc_status("NEW");
        document.setDoc_type("LP_INTRODUCE_GOODS");
        document.setImportRequest(true);
        document.setOwner_inn("1234567890");
        document.setParticipant_inn("0987654321");
        document.setProducer_inn("1122334455");
        document.setProduction_date("2024-06-25");
        document.setProduction_type("TYPE");

        Document.Description description = new Document.Description();
        description.setParticipantInn("ParticipantINN");
        document.setDescription(description);

        Document.Product product = new Document.Product();
        product.setCertificate_document("CERT_DOC");
        product.setCertificate_document_date("2024-06-25");
        product.setCertificate_document_number("CERT_NUM");
        product.setOwner_inn("1234567890");
        product.setProducer_inn("1122334455");
        product.setProduction_date("2023-01-01");
        product.setTnved_code("TNVED");
        product.setUit_code("UIT");
        product.setUitu_code("UITU");
        document.setProducts(new Document.Product[] { product });

        CrptApi crptApi = new CrptApi(4, Duration.ofSeconds(3));

        for (int i = 0; i < 10; i++) {
            int count = i+1;
            new Thread(() -> {
                try {
                    crptApi.createDocuments(document, "Signature - " + count);
                } catch (IOException | InterruptedException e) {
                    log.error(e);
                }
            }).start();
        }
    }
}


@Data
@NoArgsConstructor
class Document {
    private Description description;
    private String doc_id;
    private String doc_status;
    private String doc_type;
    private boolean importRequest;
    private String owner_inn;
    private String participant_inn;
    private String producer_inn;
    private String production_date;
    private String production_type;
    private Product[] products;
    private String reg_date;
    private String reg_number;

    @Data
    @NoArgsConstructor
    public static class Description {
        private String participantInn;
    }

    @Data
    @NoArgsConstructor
    public static class Product {
        private String certificate_document;
        private String certificate_document_date;
        private String certificate_document_number;
        private String owner_inn;
        private String producer_inn;
        private String production_date;
        private String tnved_code;
        private String uit_code;
        private String uitu_code;
    }
}

