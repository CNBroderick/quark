/*
 * Class: org.bklab.util.SM4Util
 * Modify date: 2020/3/20 上午10:25
 * Copyright (c) 2008 - 2020. - Broderick Labs.
 */

package org.bklab.quark.util.security;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.macs.CBCBlockCipherMac;
import org.bouncycastle.crypto.macs.GMac;
import org.bouncycastle.crypto.modes.GCMBlockCipher;
import org.bouncycastle.crypto.paddings.BlockCipherPadding;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Arrays;

public class SM4Util {
    private static final String ALGORITHM_NAME = "SM4";
    private static final String ALGORITHM_NAME_ECB_PADDING = "SM4/ECB/PKCS5Padding";
    private static final String ALGORITHM_NAME_ECB_NOPADDING = "SM4/ECB/NoPadding";
    private static final String ALGORITHM_NAME_CBC_PADDING = "SM4/CBC/PKCS5Padding";
    private static final String ALGORITHM_NAME_CBC_NOPADDING = "SM4/CBC/NoPadding";
    private static final int DEFAULT_KEY_SIZE = 128;
    private static final String IV = "4B765072212F74584D3D1862317D195E";
    private static final BouncyCastleProvider provider = new BouncyCastleProvider();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println(" iv\t- create iv\n encode\t- command: encode iv password\n decode\t- command: decode iv password");
            return;
        }

        String mode = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        if ("iv".equals(mode)) {
            System.out.println(generateIV());
            return;
        }

        if ("encode".equals(mode)) {
            if (args.length < 2) {
                System.out.println("command: encode iv password");
                return;
            }
            String iv = args[0];
            for (int i = 1; i < args.length; i++) {
                String password = args[i];
                if (password == null) continue;
                System.out.println(mode + " " + password);
                System.out.println(SM4Util.encode(password, args[0]));
            }
            return;
        }

        if ("decode".equals(mode)) {
            if (args.length < 2) {
                System.out.println("command: decode iv password");
                return;
            }
            String iv = args[0];
            for (int i = 1; i < args.length; i++) {
                String password = args[i];
                if (password == null) continue;
                System.out.println(mode + " " + password);
                System.out.println(SM4Util.decode(password, args[0]));
                return;
            }
        }

        System.out.println("iv\t- create iv\n encode\t- command: encode iv password\n decode\t- command: decode iv password");
    }

    private SM4Util() {

    }

    public static String encode(String password) {
        try {
            return Hex.encodeHexString(new SM4Util().encrypt_Ecb_Padding(Hex.decodeHex(IV), password.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | BadPaddingException | DecoderException | InvalidKeyException
                | NoSuchPaddingException | NoSuchProviderException | IllegalBlockSizeException | NullPointerException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String decode(String password) {
        try {
            return new String(new SM4Util().decrypt_Ecb_Padding(Hex.decodeHex(IV), Hex.decodeHex(password)));
        } catch (NoSuchAlgorithmException | BadPaddingException | DecoderException | InvalidKeyException
                | NoSuchPaddingException | NoSuchProviderException | IllegalBlockSizeException | NullPointerException e) {
            return null;
        }
    }

    public static String encode(String password, String IV) {
        try {
            return Hex.encodeHexString(new SM4Util().encrypt_Ecb_Padding(Hex.decodeHex(IV), password.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | BadPaddingException | DecoderException | InvalidKeyException
                | NoSuchPaddingException | NoSuchProviderException | IllegalBlockSizeException | NullPointerException e) {
            return null;
        }
    }

    public static String decode(String password, int[][] IV) {
        return decode(password, decodeIv(IV));
    }

    public static String encode(String password, int[][] IV) {
        return encode(password, decodeIv(IV));
    }

    public static String decode(String password, String IV) {
        try {
            return new String(new SM4Util().decrypt_Ecb_Padding(Hex.decodeHex(IV), Hex.decodeHex(password)));
        } catch (NoSuchAlgorithmException | BadPaddingException | DecoderException | InvalidKeyException
                | NoSuchPaddingException | NoSuchProviderException | IllegalBlockSizeException | NullPointerException e) {
            return null;
        }
    }

    public static String generateIV() {
        try {
            return Hex.encodeHexString(new SM4Util().generateKey());
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            return null;
        }
    }

    private static byte[] doMac(org.bouncycastle.crypto.Mac mac, byte[] key, byte[] iv, byte[] data) {
        CipherParameters cipherParameters = new KeyParameter(key);
        mac.init(new ParametersWithIV(cipherParameters, iv));
        mac.update(data, 0, data.length);
        byte[] result = new byte[mac.getMacSize()];
        mac.doFinal(result, 0);
        return result;
    }

    private static byte[] doMac(String algorithmName, Key key, byte[] data) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance(algorithmName, provider);
        mac.init(key);
        mac.update(data);
        return mac.doFinal();
    }

    private static Cipher generateEcbCipher(String algorithmName, int mode, byte[] key)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        Cipher cipher = Cipher.getInstance(algorithmName, provider);
        Key sm4Key = new SecretKeySpec(key, ALGORITHM_NAME);
        cipher.init(mode, sm4Key);
        return cipher;
    }

    private static Cipher generateCbcCipher(String algorithmName, int mode, byte[] key, byte[] iv)
            throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchPaddingException {
        Cipher cipher = Cipher.getInstance(algorithmName, provider);
        Key sm4Key = new SecretKeySpec(key, ALGORITHM_NAME);
        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
        cipher.init(mode, sm4Key, ivParameterSpec);
        return cipher;
    }

    public static String encodeIv(String iv) {
        int[][] a = new int[8][4];
        int x = 0;
        int y = 0;
        for (char c : iv.toCharArray()) {
            a[x][y] = (int) Math.pow(x + 1, y + 1) * Integer.parseInt("" + c, 16);
            if (++y > 3) {
                y = 0;
                ++x;
            }
        }
        return "int[][] IV = new int[][]" + Arrays.deepToString(a)
                .replaceAll("\\[", "{")
                .replaceAll("]", "}")
                + ";";
    }

    public static String decodeIv(int[][] b) {
        StringBuilder iv = new StringBuilder();
        for (int x = 1; x <= b.length; x++) {
            for (int y = 1; y <= b[x - 1].length; y++) {
                iv.append(Integer.toHexString(b[x - 1][y - 1] / (int) Math.pow(x, y)));
            }
        }
        return iv.toString();
    }

    private byte[] generateKey() throws NoSuchAlgorithmException, NoSuchProviderException {
        return generateKey(DEFAULT_KEY_SIZE);
    }

    private byte[] generateKey(int keySize) throws NoSuchAlgorithmException {
        KeyGenerator kg = KeyGenerator.getInstance(ALGORITHM_NAME, provider);
        kg.init(keySize, new SecureRandom());
        return kg.generateKey().getEncoded();
    }

    private byte[] encrypt_Ecb_Padding(byte[] key, byte[] data)
            throws InvalidKeyException, NoSuchAlgorithmException, NoSuchProviderException,
                   NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    private byte[] decrypt_Ecb_Padding(byte[] key, byte[] cipherText)
            throws IllegalBlockSizeException, BadPaddingException, InvalidKeyException,
            NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_PADDING, Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(cipherText);
    }

    private byte[] encrypt_Ecb_NoPadding(byte[] key, byte[] data)
            throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_NOPADDING, Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    private byte[] decrypt_Ecb_NoPadding(byte[] key, byte[] cipherText)
            throws IllegalBlockSizeException, BadPaddingException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
        Cipher cipher = generateEcbCipher(ALGORITHM_NAME_ECB_NOPADDING, Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(cipherText);
    }

    private byte[] encrypt_Cbc_Padding(byte[] key, byte[] iv, byte[] data)
            throws InvalidKeyException, NoSuchAlgorithmException,
                   NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
        Cipher cipher = generateCbcCipher(ALGORITHM_NAME_CBC_PADDING, Cipher.ENCRYPT_MODE, key, iv);
        return cipher.doFinal(data);
    }

    private byte[] decrypt_Cbc_Padding(byte[] key, byte[] iv, byte[] cipherText)
            throws IllegalBlockSizeException, BadPaddingException, InvalidKeyException,
                   NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException {
        Cipher cipher = generateCbcCipher(ALGORITHM_NAME_CBC_PADDING, Cipher.DECRYPT_MODE, key, iv);
        return cipher.doFinal(cipherText);
    }

    private byte[] encrypt_Cbc_NoPadding(byte[] key, byte[] iv, byte[] data)
            throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException,
                   InvalidAlgorithmParameterException {
        Cipher cipher = generateCbcCipher(ALGORITHM_NAME_CBC_NOPADDING, Cipher.ENCRYPT_MODE, key, iv);
        return cipher.doFinal(data);
    }

    private byte[] decrypt_Cbc_NoPadding(byte[] key, byte[] iv, byte[] cipherText)
            throws IllegalBlockSizeException, BadPaddingException, InvalidKeyException,
                   NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException {
        Cipher cipher = generateCbcCipher(ALGORITHM_NAME_CBC_NOPADDING, Cipher.DECRYPT_MODE, key, iv);
        return cipher.doFinal(cipherText);
    }

    private byte[] doCMac(byte[] key, byte[] data) throws NoSuchAlgorithmException, InvalidKeyException {
        Key keyObj = new SecretKeySpec(key, ALGORITHM_NAME);
        return doMac("SM4-CMAC", keyObj, data);
    }

    private byte[] doGMac(byte[] key, byte[] iv, int tagLength, byte[] data) {
        org.bouncycastle.crypto.Mac mac = new GMac(new GCMBlockCipher(new SM4Engine()), tagLength * 8);
        return doMac(mac, key, iv, data);
    }

    /**
     * 默认使用PKCS7Padding/PKCS5Padding填充的CBCMAC
     *
     * @param key  key
     * @param iv   iv
     * @param data data
     *
     * @return bytes
     */
    private byte[] doCBCMac(byte[] key, byte[] iv, byte[] data) {
        SM4Engine engine = new SM4Engine();
        org.bouncycastle.crypto.Mac mac = new CBCBlockCipherMac(engine, engine.getBlockSize() * 8, new PKCS7Padding());
        return doMac(mac, key, iv, data);
    }

    /**
     * @param key key
     * @param iv iv
     * @param padding 可以传null，传null表示NoPadding，由调用方保证数据必须是BlockSize的整数倍
     * @param data data
     * @return bytes
     * @throws Exception exception
     */
    private byte[] doCBCMac(byte[] key, byte[] iv, BlockCipherPadding padding, byte[] data) throws Exception {
        SM4Engine engine = new SM4Engine();
        if (padding == null) {
            if (data.length % engine.getBlockSize() != 0) {
                throw new Exception("if no padding, data length must be multiple of SM4 BlockSize");
            }
        }
        org.bouncycastle.crypto.Mac mac = new CBCBlockCipherMac(engine, engine.getBlockSize() * 8, padding);
        return doMac(mac, key, iv, data);
    }
}
