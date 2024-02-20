#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using BTCPayApp.CommonServer;
using BTCPayServer.Abstractions.Constants;
using BTCPayServer.Client;
using BTCPayServer.Common;
using BTCPayServer.Controllers;
using BTCPayServer.Data;
using BTCPayServer.Security.Greenfield;
using BTCPayServer.Services;
using BTCPayServer.Services.Stores;
using Microsoft.AspNetCore.Authentication.BearerToken;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NBitcoin;
using NBitcoin.DataEncoders;
using NBXplorer;
using NicolasDorier.RateLimits;

namespace BTCPayServer.App;

[ApiController]
[Authorize(AuthenticationSchemes = AuthenticationSchemes.Bearer)]
[Route("btcpayapp")]
public class BtcPayAppController(
    BtcPayAppService appService,
    APIKeyRepository apiKeyRepository,
    StoreRepository storeRepository,
    BTCPayNetworkProvider btcPayNetworkProvider,
    IExplorerClientProvider explorerClientProvider,
    SignInManager<ApplicationUser> signInManager,
    UserManager<ApplicationUser> userManager,
    TimeProvider timeProvider,
    IOptionsMonitor<BearerTokenOptions> bearerTokenOptions)
    : Controller
{
    [AllowAnonymous]
    [HttpPost("login")]
    [RateLimitsFilter(ZoneLimits.Login, Scope = RateLimitsScope.RemoteAddress)]
    public async Task<Results<Ok<AccessTokenResponse>, EmptyHttpResult, ProblemHttpResult>> Login(LoginRequest login)
    {
        const string errorMessage = "Invalid login attempt.";
        if (ModelState.IsValid)
        {
            // Require the user to pass basic checks (approval, confirmed email, not disabled) before they can log on
            var user = await userManager.FindByEmailAsync(login.Email);
            if (!UserService.TryCanLogin(user, out var message))
            {
                return TypedResults.Problem(message, statusCode: 401);
            }

            signInManager.AuthenticationScheme = AuthenticationSchemes.Bearer;
            var signInResult = await signInManager.CheckPasswordSignInAsync(user, login.Password, true);
            if (signInResult.Succeeded)
            { 
                await signInManager.SignInWithClaimsAsync(user, true, new List<Claim>());
                if (signInResult.RequiresTwoFactor)
                {
                    if (!string.IsNullOrEmpty(login.TwoFactorCode))
                        signInResult = await signInManager.TwoFactorAuthenticatorSignInAsync(login.TwoFactorCode, true, true);
                    else if (!string.IsNullOrEmpty(login.TwoFactorRecoveryCode))
                        signInResult = await signInManager.TwoFactorRecoveryCodeSignInAsync(login.TwoFactorRecoveryCode);
                }
                Results<Ok<AccessTokenResponse>, EmptyHttpResult, ProblemHttpResult> results = signInResult.Succeeded
                    ? TypedResults.Empty
                    : TypedResults.Problem(signInResult.ToString(), statusCode: 401);
                return results;
            }
        }
        return TypedResults.Problem(errorMessage, statusCode: 401);
    }

    [HttpPost("refresh")]
    public async Task<Results<Ok<AccessTokenResponse>, UnauthorizedHttpResult, SignInHttpResult, ChallengeHttpResult>> Refresh(RefreshRequest refresh)
    {
        const string scheme = AuthenticationSchemes.Bearer;
        var authenticationTicket = bearerTokenOptions.Get(scheme).RefreshTokenProtector.Unprotect(refresh.RefreshToken);
        var expiresUtc = authenticationTicket?.Properties.ExpiresUtc;

        ApplicationUser? user = null;
        int num;
        if (expiresUtc.HasValue)
        {
            DateTimeOffset valueOrDefault = expiresUtc.GetValueOrDefault();
            num = timeProvider.GetUtcNow() >= valueOrDefault ? 1 : 0;
        }
        else
            num = 1;
        bool flag = num != 0;
        if (!flag)
        {
            signInManager.AuthenticationScheme = scheme;
            user = await signInManager.ValidateSecurityStampAsync(authenticationTicket?.Principal);
        }
        
        return user != null
            ? TypedResults.SignIn(await signInManager.CreateUserPrincipalAsync(user), authenticationScheme: scheme)
            : TypedResults.Challenge(authenticationSchemes: new[] { scheme });
    }

    [HttpPost("logout")]
    public async Task<IResult> Logout()
    {
        var user = await userManager.GetUserAsync(User);
        if (user != null)
        {
            await signInManager.SignOutAsync();
            return Results.Ok();
        }
        return Results.Unauthorized();
    }

    [HttpGet("info")]
    public async Task<Results<Ok<InfoResponse>, ValidationProblem, NotFound>> Info()
    {
        var user = await userManager.GetUserAsync(User);
        if (user == null) return TypedResults.NotFound();
        
        return TypedResults.Ok(new InfoResponse
        {
            Email = await userManager.GetEmailAsync(user) ?? string.Empty,
            IsEmailConfirmed = await userManager.IsEmailConfirmedAsync(user)
        });
    }

    [HttpGet("pair/{code}")]
    public async Task<IActionResult> StartPair(string code)
    {
        var res = appService.ConsumePairingCode(code);
        if (res is null)
        {
            return Unauthorized();
        }

        StoreData? store = null;
        if (res.StoreId is not null)
        {
            store = await storeRepository.FindStore(res.StoreId, res.UserId);
            if (store is null)
            {
                return NotFound();
            }
        }
        

        var key = new APIKeyData()
        {
            Id = Encoders.Hex.EncodeData(RandomUtils.GetBytes(20)),
            Type = APIKeyType.Permanent,
            UserId = res.UserId,
            Label = "BTCPay App Pairing"
        };
        key.SetBlob(new APIKeyBlob() {Permissions = new[] {Policies.Unrestricted}});
        await apiKeyRepository.CreateKey(key);


        var onchain = store?.GetDerivationSchemeSettings(btcPayNetworkProvider, "BTC");
        string? onchainSeed = null;
        if (onchain is not null)
        {
            var explorerClient = explorerClientProvider.GetExplorerClient("BTC");
            onchainSeed = await GetSeed(explorerClient, onchain);
        }

        return Ok(new PairSuccessResult()
        {
            Key = key.Id,
            StoreId = store?.Id,
            UserId = res.UserId,
            ExistingWallet =
                onchain?.AccountDerivation?.GetExtPubKeys()?.FirstOrDefault()
                    ?.ToString(onchain.Network.NBitcoinNetwork),
            ExistingWalletSeed = onchainSeed,
            Network = btcPayNetworkProvider.GetNetwork<BTCPayNetwork>("BTC").NBitcoinNetwork.Name
        });
    }

    private async Task<string?> GetSeed(ExplorerClient client, DerivationSchemeSettings derivation)
    {
        return derivation.IsHotWallet &&
               await client.GetMetadataAsync<string>(derivation.AccountDerivation, WellknownMetadataKeys.Mnemonic) is
                   { } seed &&
               !string.IsNullOrEmpty(seed)
            ? seed
            : null;
    }
}
