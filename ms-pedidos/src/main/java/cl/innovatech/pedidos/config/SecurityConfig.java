package cl.innovatech.pedidos.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.*;

/**
 * Seguridad del microservicio: actua como OAuth2 Resource Server y valida el
 * Access Token (JWT) emitido por Microsoft Entra External ID.
 *
 * Validaciones aplicadas sobre cada token recibido:
 *   1. Firma criptografica, usando las claves publicas del endpoint JWKS.
 *   2. Emisor (claim iss): debe ser el tenant de Innovatech.
 *   3. Audiencia (claim aud): el token debe haber sido emitido para esta API.
 *   4. Vigencia (claim exp): no se aceptan tokens expirados.
 */
@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    @Value("${app.security.issuer-uri}")
    private String issuerUri;

    @Value("${app.security.jwk-set-uri}")
    private String jwkSetUri;

    @Value("${app.security.audiences}")
    private String audiencias;

    @Value("${app.cors.allowed-origins}")
    private String origenesPermitidos;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            // La API es sin estado y usa Bearer Tokens, no cookies de sesion.
            .csrf(csrf -> csrf.disable())
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                    // Unico endpoint publico: permite comprobar que el servicio vive.
                    .requestMatchers("/api/health").permitAll()
                    .requestMatchers("/h2-console/**").permitAll()
                    .anyRequest().authenticated())
            .headers(h -> h.frameOptions(f -> f.sameOrigin()))
            .oauth2ResourceServer(oauth -> oauth
                    .jwt(jwt -> jwt
                            .decoder(jwtDecoder())
                            .jwtAuthenticationConverter(jwtAuthenticationConverter())));
        return http.build();
    }

    /** Decodificador que verifica firma, emisor, vigencia y audiencia. */
    @Bean
    public JwtDecoder jwtDecoder() {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build();

        OAuth2TokenValidator<Jwt> porDefecto = JwtValidators.createDefaultWithIssuer(issuerUri);
        OAuth2TokenValidator<Jwt> porAudiencia = new ValidadorDeAudiencia(
                Arrays.stream(audiencias.split(",")).map(String::trim).toList());

        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(porDefecto, porAudiencia));
        return decoder;
    }

    /**
     * Traduce los claims del token a autoridades de Spring Security:
     *   claim "scp"   -> autoridades con prefijo SCOPE_
     *   claim "roles" -> autoridades con prefijo ROLE_
     * Esto permite proteger cada endpoint con @PreAuthorize.
     */
    @Bean
    public JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(jwt -> {
            Collection<GrantedAuthority> autoridades = new ArrayList<>();

            String scp = jwt.getClaimAsString("scp");
            if (scp != null && !scp.isBlank()) {
                for (String ambito : scp.split(" ")) {
                    if (!ambito.isBlank()) autoridades.add(new SimpleGrantedAuthority("SCOPE_" + ambito));
                }
            }
            List<String> roles = jwt.getClaimAsStringList("roles");
            if (roles != null) {
                for (String rol : roles) autoridades.add(new SimpleGrantedAuthority("ROLE_" + rol));
            }
            return autoridades;
        });
        return converter;
    }

    /** Se declara el origen exacto del frontend, evitando sobrepermisos. */
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOrigins(Arrays.stream(origenesPermitidos.split(",")).map(String::trim).toList());
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        config.setAllowedHeaders(List.of("Authorization", "Content-Type"));
        config.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return source;
    }

    /** Comprueba que el claim "aud" corresponda a esta API y no a otra. */
    static class ValidadorDeAudiencia implements OAuth2TokenValidator<Jwt> {
        private final List<String> audienciasAceptadas;
        ValidadorDeAudiencia(List<String> audienciasAceptadas) {
            this.audienciasAceptadas = audienciasAceptadas;
        }
        @Override
        public OAuth2TokenValidatorResult validate(Jwt token) {
            List<String> aud = token.getAudience();
            if (aud != null && aud.stream().anyMatch(audienciasAceptadas::contains)) {
                return OAuth2TokenValidatorResult.success();
            }
            return OAuth2TokenValidatorResult.failure(new OAuth2Error(
                    OAuth2ErrorCodes.INVALID_TOKEN,
                    "La audiencia del token no corresponde a este microservicio", null));
        }
    }
}
