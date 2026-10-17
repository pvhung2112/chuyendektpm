let walletBalance = 2000000; // Khởi tạo 2.000.000 VNĐ

function getBalance() {
    return walletBalance;
}

function getBalanceFormatted() {
    return `${walletBalance.toLocaleString('vi-VN')} đ`;
}

function recharge() {
    walletBalance = 2000000;
    return walletBalance;
}

function tryDeduct(price) {
    if (walletBalance >= price) {
        walletBalance -= price;
        return {
            success: true,
            balance: walletBalance,
            balanceFormatted: `${walletBalance.toLocaleString('vi-VN')} đ`
        };
    } else {
        return {
            success: false,
            balance: walletBalance,
            balanceFormatted: `${walletBalance.toLocaleString('vi-VN')} đ`,
            deficit: price - walletBalance
        };
    }
}

module.exports = {
    getBalance,
    getBalanceFormatted,
    recharge,
    tryDeduct
};
