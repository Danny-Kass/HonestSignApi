package ru.dk.crptapi;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.RateLimiter;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CrptApi {
    ObjectMapper objectMapper;
    CrptEndpoints crptEndpoints;
    Authenticator authenticator;
    HttpClient httpClient;
    Base64Encoder base64Encoder;
    RateLimiter rateLimiter;

    /**
     * Класс для работы с API Честного знака.
     * Класс thread-safe и поддерживает ограничение на количество запросов к API.
     * Ограничение указывается в конструкторе в виде количества запросов в определенный интервал времени.
     * Также в конструктор необходимо передать реализацию интерфейса Signer,
     * которая будет подписывать УКЭП зарегистрированного УОТ случайные данные (data) (ЭП присоединенная)
     * при получении аутентификационного токена
     * @param timeUnit промежуток времени.
     * @param requestLimit положительное значение, которое определяет максимальное количество запросов в этом промежутке времени.
     * @param signer реализация интерфейса Signer.
     * @throws IllegalArgumentException отрицательный {@code requestLimit}
     */
    public CrptApi(TimeUnit timeUnit, int requestLimit, Signer signer) {
        if (requestLimit <= 0) {
            throw new IllegalArgumentException("requestLimit must be positive!");
        }
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        crptEndpoints = new ProdCrptEndpoints();
        httpClient = HttpClient.newHttpClient();


        rateLimiter = RateLimiter.create((double) requestLimit / Duration.of(1, timeUnit.toChronoUnit()).toSeconds());

        base64Encoder = new JavaUtilBase64Encoder();
        authenticator = new CachingAuthenticator(
                httpClient,
                objectMapper,
                signer,
                base64Encoder,
                Duration.of(10, ChronoUnit.HOURS),
                rateLimiter,
                crptEndpoints
        );
    }

    /**
     * Метод создания документа для ввода в оборот товара, произведенного в РФ
     * @param documentData объект документа
     * @param signature открепленная подпись (УКЭП).
     * @return строка - уникальный идентификатор документа в ИС МП.
     * @throws DocumentRegistrationException ошибка при запросе на регистрацию документа
     * @throws GettingAuthDataException ошибка при запросе данных для аутентификации
     * @throws GettingTokenException ошибка при запросе токена аутентификации
     */
    public String createDocument(DocumentData documentData, String signature) {
        String token = authenticator.getToken();
        Document document = new SignedCrptDocument(
                crptEndpoints,
                objectMapper,
                base64Encoder,
                documentData,
                DocumentType.LP_INTRODUCE_GOODS,
                httpClient,
                Optional.of("milk"), // здесь задаётся необязательный параметр (товарная группа)
                signature,
                token,
                rateLimiter
        );
        return document.register();
    }

    public interface Signer {
        String sign(String data);
    }

    private interface CrptEndpoints {
        String createDocumentUrl(Optional<String> productGroup);

        String getAuthDataUrl();

        String getTokenUrl();
    }

    private static class ProdCrptEndpoints implements CrptEndpoints {
        @Override
         public String createDocumentUrl(Optional<String> productGroup) {
            return Constants.URL_CREATE_DOCUMENT + productGroup.map(s -> "?pg=" + s).orElse("");
        }

        @Override
        public String getAuthDataUrl() {
            return Constants.URL_AUTHORIZATION;
        }

        @Override
        public String getTokenUrl() {
            return Constants.URL_TOKEN;
        }
    }

    private interface Base64Encoder {
        String encode(String data);

        String decode(String encodedData);
    }

    private static class JavaUtilBase64Encoder implements Base64Encoder {
        @Override
        public String encode(String data) {
            return new String(Base64.getEncoder().encode(data.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        }

        @Override
        public String decode(String encodedData) {
            return new String(Base64.getDecoder().decode(encodedData.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        }
    }

    private interface Document {
        String register();
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @AllArgsConstructor
    private static class SignedCrptDocument implements Document {
        CrptEndpoints crptEndpoints;
        ObjectMapper objectMapper;
        Base64Encoder b64encoder;
        DocumentData documentData;
        DocumentType documentType;
        HttpClient httpClient;
        Optional<String> productGroup;
        String signature;
        String token;
        RateLimiter rateLimiter;

        @Override
        public String register() {
            try {
                String documentJson = objectMapper.writeValueAsString(documentData);
                String signatureB64 = b64encoder.encode(signature);
                String documentJsonB64 = b64encoder.encode(documentJson);
                ReqBody reqBody = new ReqBody(
                        Constants.DOCUMENT_FORMAT_MANUAL,
                        documentJsonB64,
                        productGroup,
                        signatureB64,
                        documentType.name()
                );
                HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(crptEndpoints.createDocumentUrl(productGroup)))
                        .header(Constants.HEADER_AUTHORIZATION, "Bearer " + token)
                        .header(Constants.HEADER_CONTENT_TYPE, Constants.APPLICATION_JSON)
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(reqBody)))
                        .build();
                rateLimiter.acquire();
                HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                return objectMapper.readValue(response.body(), UuidDocumentResponse.class).getValue();
            } catch (IOException | InterruptedException e) {
                throw new DocumentRegistrationException(e);
            }
        }
    }


    private interface Authenticator {
        String getToken();
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    @RequiredArgsConstructor
    private static class CachingAuthenticator implements Authenticator {
        final HttpClient httpClient;
        final ObjectMapper objectMapper;
        final Signer signer;
        final Base64Encoder base64Encoder;
        String cachedToken;
        LocalDateTime authDeadline = LocalDateTime.MIN;
        final Duration tokenLifeTime;
        final RateLimiter rateLimiter;

        final CrptEndpoints crptEndpoints;

        @Override
        public String getToken() {
            if (LocalDateTime.now().isAfter(authDeadline)) {
                LocalDateTime now = LocalDateTime.now();
                cachedToken = renewToken();
                authDeadline = now.plus(tokenLifeTime);
            }
            return cachedToken;
        }

        private String renewToken() {
            AuthData authData = getAuthData();
            AuthData signedData = signData(authData);
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(crptEndpoints.getTokenUrl()))
                        .header(Constants.HEADER_CONTENT_TYPE, Constants.APPLICATION_JSON)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(signedData)))
                        .build();
                rateLimiter.acquire();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                TokenResponse tokenResponse = objectMapper
                        .readValue(response.body(), TokenResponse.class);
                return tokenResponse.getToken();
            } catch (InterruptedException | IOException e) {
                throw new GettingTokenException(e);
            }
        }

        private AuthData getAuthData() {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(crptEndpoints.getAuthDataUrl()))
                    .GET()
                    .build();
            try {
                rateLimiter.acquire();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                return objectMapper
                        .readValue(response.body(), AuthData.class);
            } catch (IOException | InterruptedException e) {
                throw new GettingAuthDataException(e);
            }
        }

        private AuthData signData(AuthData authData) {
            String signedData = signer.sign(authData.getData());
            return new AuthData(authData.getUuid(), base64Encoder.encode(signedData));
        }
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    private static class AuthData {
        String uuid;
        String data;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    private static class TokenResponse {
        String token;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    private static class UuidDocumentResponse {
        String value;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    private static class Constants {
        static String URL_AUTHORIZATION = "https://ismp.crpt.ru/api/v3/auth/cert/key";
        static String URL_TOKEN = "https://ismp.crpt.ru/api/v3/auth/cert/";
        static String URL_CREATE_DOCUMENT = "https://ismp.crpt.ru/api/v3/lk/documents/create";
        static String HEADER_AUTHORIZATION = "Authorization";
        static String HEADER_CONTENT_TYPE = "Content-Type";
        static String APPLICATION_JSON = "application/json;charset=UTF-8";
        static String DOCUMENT_FORMAT_MANUAL = "MANUAL";
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @AllArgsConstructor
    @Getter
    private static class ReqBody {
        String document_format;
        String product_document;
        Optional<String> product_group;
        String signature;
        String type;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @AllArgsConstructor
    @Getter
    // Все поля - не null. Пустые поля задаются с помощью Optional.empty()
    public static class DocumentData {
        Optional<Description> description;
        String doc_id;
        String doc_status;
        String doc_type;
        Optional<Boolean> importRequest;
        String participant_inn;
        String producer_inn;
        LocalDate production_date;
        String production_type;
        Optional<List<Product>> products;
        LocalDate reg_date;
        Optional<String> reg_number;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @AllArgsConstructor
    @Getter
    public static class Product {
        Optional<String> certificate_document;
        Optional<LocalDate> certificate_document_date;
        Optional<String> certificate_document_number;
        String owner_inn;
        String producer_inn;
        LocalDate production_date;
        String tnved_code;
        Optional<String> uit_code;
        Optional<String> uitu_code;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @AllArgsConstructor
    @Getter
    public static class Description {
        String participantInn;
    }

    public static class DocumentRegistrationException extends RuntimeException {
        public DocumentRegistrationException(Throwable cause) {
            super(cause);
        }
    }

    private abstract static class AuthException extends RuntimeException {
        public AuthException(Throwable cause) {
            super(cause);
        }
    }

    public static class GettingAuthDataException extends AuthException {
        public GettingAuthDataException(Throwable cause) {
            super(cause);
        }
    }

    public static class GettingTokenException extends AuthException {
        public GettingTokenException(Throwable cause) {
            super(cause);
        }
    }

    private enum DocumentType {
        AGGREGATION_DOCUMENT,
        DISAGGREGATION_DOCUMENT,
        REAGGREGATION_DOCUMENT,
        LP_INTRODUCE_GOODS,
        LP_SHIP_GOODS,
        LP_ACCEPT_GOODS,
        LK_REMARK,
        LK_RECEIPT,
        LP_GOODS_IMPORT,
        LP_CANCEL_SHIPMENT,
        LK_KM_CANCELLATION,
        LK_APPLIED_KM_CANCELLATION,
        LK_CONTRACT_COMMISSIONING,
        LK_INDI_COMMISSIONING,
        LP_SHIP_RECEIPT,
        OST_DESCRIPTION,
        CROSSBORDER,
        LP_INTRODUCE_OST,
        LP_RETURN,
        LP_SHIP_GOODS_CROSSBORDER,
        LP_CANCEL_SHIPMENT_CROSSBORDER
    }
}
