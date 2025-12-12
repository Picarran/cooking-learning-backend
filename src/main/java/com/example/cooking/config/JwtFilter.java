package com.example.cooking.config;

import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dto.resp.Result;
import com.example.cooking.utils.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class JwtFilter extends OncePerRequestFilter {

    private final JwtUtil jwtUtil;
    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    public JwtFilter(JwtUtil jwtUtil) {
        this.jwtUtil = jwtUtil;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {
        String path = request.getRequestURI();
        // do not filter auth endpoints and static resources
        return pathMatcher.match("/api/auth/**", path) || path.startsWith("/static") || path.startsWith("/favicon.ico");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        try {
            if (header != null && header.startsWith("Bearer ")) {
                String token = header.substring(7);
                try {
                    Jws<Claims> claims = jwtUtil.parseToken(token);
                    String sub = claims.getBody().getSubject();
                    if (sub != null) {
                        request.setAttribute("currentUserId", Long.parseLong(sub));
                    }
                } catch (JwtException e) {
                    writeError(response, Result.buildFailure("非法token或token过期!"), HttpServletResponse.SC_UNAUTHORIZED);
                    return;
                }
            } else {
                writeError(response, Result.buildFailure("未携带登录token!"), HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }

            filterChain.doFilter(request, response);
        } catch (CookingException e) {
            writeError(response, Result.buildFailure(e.getMessage()), HttpServletResponse.SC_UNAUTHORIZED);
        } catch (Exception e) {
            writeError(response, Result.buildFailure(e.getMessage()), HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    private void writeError(HttpServletResponse response, Result<?> result, int status) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json;charset=UTF-8");
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(response.getWriter(), result);
    }
}
