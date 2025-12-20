package com.zhugeio.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/**
 * AES加解密工具类
 *
 * 提供AES加密、解密以及密钥生成功能，使用CBC模式，PKCS5Padding填充方式
 *
 * 主要功能：
 * 1. 生成符合AES要求的随机密钥
 * 2. 对字符串进行AES加密
 * 3. 对AES加密结果进行解密
 */
@Slf4j
@Component
public class AESUtils {

    private final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";
    private final String KEY_ALGORITHM = "AES";
    private final String UTF8_CHARSET = "UTF-8";

    private final String ivParameter = "1236547899874560";

    /**
     * 产生符合要求的Key,随机性更好
     * @return
     * @throws NoSuchAlgorithmException
     */
    public String generateKey() throws NoSuchAlgorithmException, UnsupportedEncodingException {
        KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);
        SecureRandom secureRandom = new SecureRandom(
                String.valueOf(System.currentTimeMillis()).getBytes(UTF8_CHARSET));
        kg.init(256,secureRandom);
        SecretKey secretKey = kg.generateKey();
        byte[] keyBytes = Arrays.copyOfRange(secretKey.getEncoded(),0,16);
        return Base64.getEncoder().encodeToString(keyBytes);
    }

    /**
     * 获取算法需要的安全密钥
     * @param key
     * @return
     */
    private SecretKeySpec getSecretKey(String key){
        byte[] bytes = Base64.getDecoder().decode(key);
        return new SecretKeySpec(bytes, KEY_ALGORITHM);
    }

    /**
     * 加密
     * @param input
     * @param key
     * @return
     */
    public String encrypt(String input, String key) throws Exception {
        if (input == null || key == null){
            return null;
        }
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
        cipher.init(Cipher.ENCRYPT_MODE, getSecretKey(key), iv);
        byte[] encrypted = cipher.doFinal(input.getBytes(UTF8_CHARSET));
        return new BASE64Encoder().encode(encrypted);
    }

    /**
     * 解密
     * @param cipherText
     * @param key
     * @return
     */
    public String decrypt(String cipherText, String key) throws Exception {
        if (cipherText == null || key == null) {
            return null;
        }
        SecretKeySpec secretKey = getSecretKey(key);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
        cipher.init(Cipher.DECRYPT_MODE, secretKey , iv);
        byte[] encryptedBytes = new BASE64Decoder().decodeBuffer(cipherText);
        byte[] original = cipher.doFinal(encryptedBytes);
        return new String(original,UTF8_CHARSET);
    }
}