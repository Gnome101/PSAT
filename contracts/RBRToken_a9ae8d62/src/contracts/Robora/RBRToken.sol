/**
    X:          https://x.com/UseRobora
    Telegram:   https://t.me/roboratg
    Website:    https://robora.xyz/
    Docs:       https://robora.gitbook.io/robora-docs/
*/

// SPDX-License-Identifier: MIT

pragma solidity 0.8.30;

import "@openzeppelin/contracts@5.3.0/access/Ownable.sol";
import "@openzeppelin/contracts@5.3.0/token/ERC20/ERC20.sol";
import "./interfaces/IUniswapV2Factory.sol";
import "./interfaces/IUniswapV2Router.sol";

contract RBRToken is Ownable, ERC20 {
    IUniswapV2Router public immutable uniswapV2Router;

    address public constant ZERO_ADDRESS = address(0);
    address public constant DEAD_ADDRESS = address(0xdEaD);

    address public immutable uniswapV2Pair;
    address public operationsWallet;
    address public developmentWallet;

    bool public isLimitsEnabled;
    bool public isCooldownEnabled;
    bool public isTaxEnabled;
    bool private _inSwapBack;
    bool public isLaunched;

    uint256 private _lastSwapBackExecutionBlock;

    uint256 public constant MAX_FEES = 20;

    uint256 public maxBuy;
    uint256 public maxSell;
    uint256 public maxWallet;

    uint256 public swapTokensAtAmount;
    uint256 public buyFee;
    uint256 public sellFee;
    uint256 public transferFee;

    uint256 private _dShares;

    mapping(address => bool) public isBot;
    mapping(address => bool) public isExcludedFromFees;
    mapping(address => bool) public isExcludedFromLimits;
    mapping(address => bool) public automatedMarketMakerPairs;
    mapping(address => uint256) private _holderLastTransferTimestamp;

    event Launch();
    event SetOperationsWallet(address newWallet, address oldWallet);
    event SetDevelopmentWallet(address newWallet, address oldWallet);
    event SetLimitsEnabled(bool status);
    event SetCooldownEnabled(bool status);
    event SetTaxesEnabled(bool status);
    event SetMaxBuy(uint256 amount);
    event SetMaxSell(uint256 amount);
    event SetMaxWallet(uint256 amount);
    event SetSwapTokensAtAmount(uint256 newValue, uint256 oldValue);
    event SetBuyFees(uint256 newValue, uint256 oldValue);
    event SetSellFees(uint256 newValue, uint256 oldValue);
    event SetTransferFees(uint256 newValue, uint256 oldValue);
    event SetDShares(uint256 newValue, uint256 oldValue);
    event ExcludeFromFees(address account, bool isExcluded);
    event ExcludeFromLimits(address account, bool isExcluded);
    event SetBots(address account, bool isExcluded);
    event SetAutomatedMarketMakerPair(address pair, bool value);
    event WithdrawStuckTokens(address token, uint256 amount);

    error AlreadyLaunched();
    error InvalidSender();
    error AddressZero();
    error AmountTooLow();
    error AmountTooHigh();
    error FeeTooHigh();
    error DShareTooLow();
    error AMMAlreadySet();
    error NoNativeTokens();
    error NoTokens();
    error FailedToWithdrawNativeTokens();
    error BotDetected();
    error TransferDelay();
    error MaxBuyAmountExceed();
    error MaxSellAmountExceed();
    error MaxWalletAmountExceed();
    error NotLaunched();

    modifier lockSwapBack() {
        _inSwapBack = true;
        _;
        _inSwapBack = false;
    }

    constructor() Ownable(msg.sender) ERC20("Robora", "RBR") {
        address sender = msg.sender;
        _mint(sender, 100_000_000 ether);
        uint256 totalSupply = totalSupply();

        operationsWallet = 0x4a066b0a5b2DBB1B9f13D0dd4896BD42c916C2fD;
        developmentWallet = 0x27791aD490DAB2cd03C71655cb477E6d764A27dC;

        address uniswapFeeCollector = 0x000000fee13a103A10D593b9AE06b3e05F2E7E1c;

        maxBuy = (totalSupply * 8) / 1000;
        maxSell = (totalSupply * 8) / 1000;
        maxWallet = (totalSupply * 8) / 1000;
        swapTokensAtAmount = (totalSupply * 5) / 10000;

        isLimitsEnabled = true;
        isCooldownEnabled = true;
        isTaxEnabled = true;

        buyFee = 20;
        sellFee = 30;
        transferFee = 60;

        _dShares = 30;

        uniswapV2Router = IUniswapV2Router(
            0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D
        );

        uniswapV2Pair = IUniswapV2Factory(uniswapV2Router.factory()).createPair(
            address(this),
            uniswapV2Router.WETH()
        );

        _setAutomatedMarketMakerPair(uniswapV2Pair, true);
        _approve(address(this), address(uniswapV2Router), type(uint256).max);
        _excludeFromFees(address(this), true);
        _excludeFromFees(DEAD_ADDRESS, true);
        _excludeFromFees(sender, true);
        _excludeFromFees(operationsWallet, true);
        _excludeFromFees(developmentWallet, true);
        _excludeFromFees(uniswapFeeCollector, true);
        _excludeFromLimits(address(this), true);
        _excludeFromLimits(DEAD_ADDRESS, true);
        _excludeFromLimits(sender, true);
        _excludeFromLimits(operationsWallet, true);
        _excludeFromLimits(developmentWallet, true);
        _excludeFromLimits(uniswapFeeCollector, true);
    }

    receive() external payable {}

    fallback() external payable {}

    function launch() external onlyOwner {
        require(!isLaunched, AlreadyLaunched());
        isLaunched = true;
        emit Launch();
    }

    function setOperationsWallet(address _operationsWallet) external {
        require(msg.sender == operationsWallet, InvalidSender());
        require(_operationsWallet != ZERO_ADDRESS, AddressZero());
        address oldWallet = operationsWallet;
        operationsWallet = _operationsWallet;
        emit SetOperationsWallet(operationsWallet, oldWallet);
    }

    function setDevelopmentWallet(address _developmentWallet) external {
        require(msg.sender == developmentWallet, InvalidSender());
        require(_developmentWallet != ZERO_ADDRESS, AddressZero());
        address oldWallet = developmentWallet;
        developmentWallet = _developmentWallet;
        emit SetDevelopmentWallet(developmentWallet, oldWallet);
    }

    function setLimitsEnabled(bool value) external onlyOwner {
        isLimitsEnabled = value;
        emit SetLimitsEnabled(value);
    }

    function setCooldownEnabled(bool value) external onlyOwner {
        isCooldownEnabled = value;
        emit SetCooldownEnabled(value);
    }

    function setTaxesEnabled(bool value) external onlyOwner {
        isTaxEnabled = value;
        emit SetTaxesEnabled(value);
    }

    function setMaxBuy(uint256 amount) external onlyOwner {
        require(amount >= ((totalSupply() * 2) / 1000), AmountTooLow());
        maxBuy = amount;
        emit SetMaxBuy(maxBuy);
    }

    function setMaxSell(uint256 amount) external onlyOwner {
        require(amount >= ((totalSupply() * 2) / 1000), AmountTooLow());
        maxSell = amount;
        emit SetMaxSell(maxSell);
    }

    function setMaxWallet(uint256 amount) external onlyOwner {
        require(amount >= ((totalSupply() * 3) / 1000), AmountTooLow());
        maxWallet = amount;
        emit SetMaxWallet(maxWallet);
    }

    function setSwapTokensAtAmount(uint256 amount) external onlyOwner {
        uint256 _totalSupply = totalSupply();
        require(amount >= (_totalSupply * 1) / 1000000, AmountTooLow());
        require(amount <= (_totalSupply * 5) / 1000, AmountTooHigh());
        uint256 oldValue = swapTokensAtAmount;
        swapTokensAtAmount = amount;
        emit SetSwapTokensAtAmount(amount, oldValue);
    }

    function setBuyFees(uint256 _buyFee) external onlyOwner {
        require(_buyFee <= MAX_FEES, FeeTooHigh());
        uint256 oldValue = buyFee;
        buyFee = _buyFee;
        emit SetBuyFees(_buyFee, oldValue);
    }

    function setSellFees(uint256 _sellFee) external onlyOwner {
        require(_sellFee <= MAX_FEES, FeeTooHigh());
        uint256 oldValue = sellFee;
        sellFee = _sellFee;
        emit SetSellFees(_sellFee, oldValue);
    }

    function setTransferFees(uint256 _transferFee) external onlyOwner {
        require(_transferFee <= MAX_FEES, FeeTooHigh());
        uint256 oldValue = transferFee;
        transferFee = _transferFee;
        emit SetTransferFees(_transferFee, oldValue);
    }

    function setDShares(uint256 dShares_) external onlyOwner {
        require(dShares_ >= 20, DShareTooLow());
        uint256 oldValue = _dShares;
        _dShares = dShares_;
        emit SetDShares(dShares_, oldValue);
    }

    function excludeFromFees(address[] calldata accounts, bool value)
        external
        onlyOwner
    {
        for (uint256 i = 0; i < accounts.length; i++) {
            _excludeFromFees(accounts[i], value);
        }
    }

    function excludeFromLimits(address[] calldata accounts, bool value)
        external
        onlyOwner
    {
        for (uint256 i = 0; i < accounts.length; i++) {
            _excludeFromLimits(accounts[i], value);
        }
    }

    function setBots(address[] calldata accounts, bool value)
        external
        onlyOwner
    {
        for (uint256 i = 0; i < accounts.length; i++) {
            if (
                (!automatedMarketMakerPairs[accounts[i]]) &&
                (accounts[i] != address(uniswapV2Router)) &&
                (accounts[i] != address(this)) &&
                (accounts[i] != ZERO_ADDRESS) &&
                (!isExcludedFromFees[accounts[i]] &&
                    !isExcludedFromLimits[accounts[i]])
            ) _setBots(accounts[i], value);
        }
    }

    function setAutomatedMarketMakerPair(address pair, bool value)
        external
        onlyOwner
    {
        require(!automatedMarketMakerPairs[pair], AMMAlreadySet());
        _setAutomatedMarketMakerPair(pair, value);
    }

    function withdrawStuckTokens(address _token) external onlyOwner {
        address sender = msg.sender;
        uint256 amount;
        if (_token == ZERO_ADDRESS) {
            bool success;
            amount = address(this).balance;
            require(amount > 0, NoNativeTokens());
            (success, ) = address(sender).call{value: amount}("");
            require(success, FailedToWithdrawNativeTokens());
        } else {
            amount = IERC20(_token).balanceOf(address(this));
            require(amount > 0, NoTokens());
            IERC20(_token).transfer(msg.sender, amount);
        }
        emit WithdrawStuckTokens(_token, amount);
    }

    function _transferOwnership(address newOwner) internal virtual override {
        address oldOwner = owner();
        if (oldOwner != ZERO_ADDRESS) {
            _excludeFromFees(oldOwner, false);
            _excludeFromLimits(oldOwner, false);
        }
        _excludeFromFees(newOwner, true);
        _excludeFromLimits(newOwner, true);
        super._transferOwnership(newOwner);
    }

    function _update(
        address from,
        address to,
        uint256 amount
    ) internal virtual override {
        address sender = msg.sender;
        address origin = tx.origin;
        uint256 blockNumber = block.number;

        require(!isBot[from], BotDetected());
        require(sender == from || !isBot[sender], BotDetected());
        require(
            origin == from || origin == sender || !isBot[origin],
            BotDetected()
        );

        require(
            isLaunched ||
                isExcludedFromLimits[from] ||
                isExcludedFromLimits[to],
            NotLaunched()
        );

        bool limits = isLimitsEnabled &&
            !_inSwapBack &&
            !(isExcludedFromLimits[from] || isExcludedFromLimits[to]);
        if (limits) {
            if (
                from != owner() &&
                to != owner() &&
                to != ZERO_ADDRESS &&
                to != DEAD_ADDRESS
            ) {
                if (isCooldownEnabled) {
                    if (to != address(uniswapV2Router) && to != uniswapV2Pair) {
                        require(
                            _holderLastTransferTimestamp[origin] <
                                blockNumber - 3 &&
                                _holderLastTransferTimestamp[to] <
                                blockNumber - 3,
                            TransferDelay()
                        );
                        _holderLastTransferTimestamp[origin] = blockNumber;
                        _holderLastTransferTimestamp[to] = blockNumber;
                    }
                }

                if (
                    automatedMarketMakerPairs[from] && !isExcludedFromLimits[to]
                ) {
                    require(amount <= maxBuy, MaxBuyAmountExceed());
                    require(
                        amount + balanceOf(to) <= maxWallet,
                        MaxWalletAmountExceed()
                    );
                } else if (
                    automatedMarketMakerPairs[to] && !isExcludedFromLimits[from]
                ) {
                    require(amount <= maxSell, MaxSellAmountExceed());
                } else if (!isExcludedFromLimits[to]) {
                    require(
                        amount + balanceOf(to) <= maxWallet,
                        MaxWalletAmountExceed()
                    );
                }
            }
        }

        bool takeFee = isTaxEnabled &&
            !_inSwapBack &&
            !(isExcludedFromFees[from] || isExcludedFromFees[to]);

        if (takeFee) {
            uint256 fees = 0;
            if (automatedMarketMakerPairs[to] && sellFee > 0) {
                fees = (amount * sellFee) / 100;
            } else if (automatedMarketMakerPairs[from] && buyFee > 0) {
                fees = (amount * buyFee) / 100;
            } else if (
                !automatedMarketMakerPairs[to] &&
                !automatedMarketMakerPairs[from] &&
                transferFee > 0
            ) {
                fees = (amount * transferFee) / 100;
            }

            if (fees > 0) {
                amount -= fees;
                super._update(from, address(this), fees);
            }
        }

        uint256 balance = balanceOf(address(this));
        bool shouldSwap = balance >= swapTokensAtAmount;
        if (takeFee && !automatedMarketMakerPairs[from] && shouldSwap) {
            if (blockNumber > _lastSwapBackExecutionBlock) {
                _swapBack(balance);
                _lastSwapBackExecutionBlock = blockNumber;
            }
        }

        super._update(from, to, amount);
    }

    function _swapBack(uint256 balance) internal virtual lockSwapBack {
        bool success;
        address[] memory path = new address[](2);
        path[0] = address(this);
        path[1] = uniswapV2Router.WETH();

        uint256 maxSwapAmount = swapTokensAtAmount * 20;

        if (balance > maxSwapAmount) {
            balance = maxSwapAmount;
        }

        uniswapV2Router.swapExactTokensForETHSupportingFeeOnTransferTokens(
            balance,
            0,
            path,
            address(this),
            block.timestamp
        );

        uint256 ethBalance = address(this).balance;

        uint256 ethForDevelopment = (ethBalance * _dShares) / 100;

        uint256 ethForOperations = ethBalance - ethForDevelopment;

        (success, ) = address(operationsWallet).call{value: ethForOperations}(
            ""
        );

        (success, ) = address(developmentWallet).call{value: ethForDevelopment}(
            ""
        );
    }

    function _excludeFromFees(address account, bool value) internal virtual {
        isExcludedFromFees[account] = value;
        emit ExcludeFromFees(account, value);
    }

    function _excludeFromLimits(address account, bool value) internal virtual {
        isExcludedFromLimits[account] = value;
        emit ExcludeFromLimits(account, value);
    }

    function _setBots(address account, bool value) internal virtual {
        isBot[account] = value;
        emit SetBots(account, value);
    }

    function _setAutomatedMarketMakerPair(address pair, bool value)
        internal
        virtual
    {
        automatedMarketMakerPairs[pair] = value;
        emit SetAutomatedMarketMakerPair(pair, value);
    }
}
